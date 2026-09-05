/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A pool of {@link PublisherLease}s - one Pulsar producer each - keyed by topic.
 * <p>
 * {@link #obtainPublisher(String)} hands out an idle lease for the topic when there is one and only creates a new
 * producer when every lease for that topic is in use. Closing a lease returns it to the pool; it does not close the
 * producer. Closing the pool closes every producer it created, idle or in use, and any lease closed after that closes
 * its producer instead of returning to the pool. Both close paths are idempotent.
 * <p>
 * Before this the idle queue was never written to, so every {@code obtainPublisher()} created a new producer, a lease's
 * {@code close()} removed it from an always-empty queue instead of closing it, and {@link #close()} drained that same
 * empty queue: every producer this bundle opened, and its broker connection, was leaked.
 * <p>
 * Under an exclusive {@code accessMode} the pool holds <b>one</b> producer per topic instead: a caller whose topic already
 * has its lease in use waits for that lease to come back rather than creating a second producer. A second producer on a
 * topic whose first one is exclusive is what the broker refuses ({@code Exclusive}), what fences the first one
 * ({@code ExclusiveWithFencing}) or what waits for a producer that never closes ({@code WaitForExclusive}), so a
 * processor with more than one concurrent task was colliding with its own pool (#219).
 */
public class PublisherPool implements Closeable {

    private final ComponentLog logger;
    private final Map<String, Object> pulsarProducerProperties;
    private final PulsarClient pulsarClient;

    /**
     * Set on the builder rather than passed in {@link #pulsarProducerProperties}, because
     * {@code loadConf} cannot carry it. That method maps the configuration through Jackson, and
     * {@code ProducerConfigurationData.batcherBuilder} is annotated {@code @JsonIgnore(true)} - the
     * client excludes it deliberately, so a builder placed in the map is dropped without an error and
     * the producer runs with the default one. Being deliberate, this will not change in a later client
     * version, and no value of any type can reach the field that way. Verified against 4.2.4: the same
     * map carrying hashingScheme sets that field and leaves this one at the default. That silent drop
     * is the same shape of bug as the two properties #180 lost. May be null, which leaves the client
     * default in place.
     */
    private final BatcherBuilder batcherBuilder;

    /** Idle leases per topic, ready to be handed out again. */
    private final Map<String, Queue<PooledPublisherLease>> idleLeases = new ConcurrentHashMap<>();

    /** Every lease created by this pool whose producer is still open, idle or in use. */
    private final Set<PooledPublisherLease> openLeases = ConcurrentHashMap.newKeySet();

    /** Whether {@code accessMode} in the producer configuration is one of the exclusive modes. */
    private final boolean exclusiveAccess;

    /**
     * One permit per topic when the access mode is exclusive, held from {@link #obtainPublisher(String)} until the
     * lease is closed. Fair, so waiting tasks are served in the order they asked.
     */
    private final Map<String, Semaphore> topicPermits = new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    public PublisherPool(ComponentLog logger, Map<String, Object> pulsarProducerProperties, PulsarClient pulsarClient) {
        this(logger, pulsarProducerProperties, pulsarClient, null);
    }

    public PublisherPool(ComponentLog logger, Map<String, Object> pulsarProducerProperties, PulsarClient pulsarClient,
                         BatcherBuilder batcherBuilder) {
        this.logger = logger;
        this.pulsarProducerProperties = pulsarProducerProperties;
        this.pulsarClient = pulsarClient;
        this.batcherBuilder = batcherBuilder;
        this.exclusiveAccess = isExclusive(pulsarProducerProperties.get("accessMode"));
    }

    /**
     * The processor puts the {@link ProducerAccessMode} itself into the map; a configuration that does not
     * mention the mode gets the client default, {@code Shared}.
     */
    private static boolean isExclusive(final Object accessMode) {
        return accessMode != null && !ProducerAccessMode.Shared.name().equals(String.valueOf(accessMode));
    }

    /**
     * Returns a lease for the topic: an idle one if available, otherwise a newly created producer. Under an
     * exclusive access mode the topic has a single lease, and a caller who finds it in use waits until it is
     * returned rather than getting a second producer.
     *
     * @param topicName the topic to publish to
     * @return the lease, or {@code null} when the topic is blank, the producer cannot be created, or the thread
     *         was interrupted while waiting for an exclusive topic's lease
     * @throws IllegalStateException if the pool has been closed
     */
    public PublisherLease obtainPublisher(String topicName) {
        if (isClosed()) {
            throw new IllegalStateException("Connection Pool is closed");
        }

        if (StringUtils.isBlank(topicName)) {
            return null;
        }

        final Semaphore permit = exclusiveAccess ? topicPermits.computeIfAbsent(topicName, t -> new Semaphore(1, true)) : null;
        if (permit != null) {
            // Exclusive access means one producer on the topic, and the pool honours that from the inside: the
            // second task wanting this topic waits for its lease to be returned instead of creating a producer the
            // broker would refuse, fence the first one with, or hold waiting for a producer that never closes. The
            // wait is on a sibling task's trigger, which the lease's own send timeout bounds.
            try {
                permit.acquire();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        try {
            final PooledPublisherLease idle = idleLeasesFor(topicName).poll();
            final PooledPublisherLease lease = idle != null ? idle : createLease(topicName);
            lease.leased.set(true);
            return lease;
        } catch (PulsarClientException pcEx) {
            logger.error("Unable to create producer", pcEx);
            if (permit != null) {
                permit.release();
            }
            return null;
        } catch (final RuntimeException e) {
            if (permit != null) {
                permit.release();
            }
            throw e;
        }
    }

    private Queue<PooledPublisherLease> idleLeasesFor(final String topicName) {
        return idleLeases.computeIfAbsent(topicName, topic -> new ConcurrentLinkedQueue<>());
    }

    private PooledPublisherLease createLease(String topicName) throws PulsarClientException {
        final Map<String, Object> properties = new HashMap<>(pulsarProducerProperties);

        // AUTO_PRODUCE_BYTES makes the broker validate the payload against whatever schema the topic
        // currently carries, instead of writing it as opaque bytes. Producing with the default BYTES schema
        // meant a topic with, say, an AVRO schema accepted arbitrary content without complaint: the message
        // landed on the topic looking valid, the registered schema was untouched, and every schema-aware
        // consumer then failed to decode it - and stayed stuck on it. On a topic with no schema this
        // behaves exactly as before and any bytes are accepted.
        // Kept so the lease can ask what schema the topic actually carries: once the producer exists this
        // schema has been bound to the topic and reports its SchemaInfo.
        final Schema<byte[]> topicSchema = Schema.AUTO_PRODUCE_BYTES();

        final ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer(topicSchema)
                .topic(topicName)
                .loadConf(properties);

        // After loadConf, so it is not overwritten by the map, and only when one was chosen.
        if (batcherBuilder != null) {
            producerBuilder.batcherBuilder(batcherBuilder);
        }

        final Producer producer = producerBuilder.create();

        final PooledPublisherLease lease = new PooledPublisherLease(producer, topicName, topicSchema);
        openLeases.add(lease);

        if (isClosed()) {
            // the pool was closed while this producer was being created: do not hand out a lease that
            // nothing would ever close
            lease.closeProducer();
            throw new IllegalStateException("Connection Pool is closed");
        }

        return lease;
    }

    /**
     * @return the number of producers currently open, idle or in use
     */
    public int getOpenProducerCount() {
        return openLeases.size();
    }

    /**
     * @return the number of producers currently idle and available to {@link #obtainPublisher(String)}
     */
    public int getIdleProducerCount() {
        return idleLeases.values().stream().mapToInt(Queue::size).sum();
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    /**
     * Closes every producer created by this pool, idle or in use. A lease that is still in use keeps working until
     * its owner closes it, at which point its producer is closed as well (once).
     */
    @Override
    public synchronized void close() {
        closed = true;
        idleLeases.clear();

        for (PooledPublisherLease lease : new ArrayList<>(openLeases)) {
            lease.closeProducer();
        }
    }

    /**
     * A lease whose {@code close()} returns the producer to the pool while the pool is open and closes it afterwards.
     */
    private final class PooledPublisherLease extends PublisherLease {
        private final String topicName;
        private final AtomicBoolean producerClosed = new AtomicBoolean(false);

        /** True while handed out by {@link #obtainPublisher(String)}; a close on a lease that is not out is ignored. */
        private final AtomicBoolean leased = new AtomicBoolean(false);

        private PooledPublisherLease(final Producer producer, final String topicName,
                                     final Schema<byte[]> topicSchema) {
            super(producer, logger, topicSchema);
            this.topicName = topicName;
        }

        @Override
        public void close() {
            if (!leased.compareAndSet(true, false)) {
                // closed twice: returning it again would put the same producer in the idle queue twice and, under
                // exclusive access, hand the topic to two tasks at once
                return;
            }

            if (PublisherPool.this.isClosed()) {
                closeProducer();
            } else {
                // return the producer to the pool; PublisherPool.close() closes it later because it stays in openLeases
                idleLeasesFor(topicName).offer(this);
            }

            if (exclusiveAccess) {
                topicPermits.get(topicName).release();
            }
        }

        private void closeProducer() {
            if (producerClosed.compareAndSet(false, true)) {
                openLeases.remove(this);
                super.close();
            }
        }
    }
}
