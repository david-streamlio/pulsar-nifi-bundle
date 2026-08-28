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
 */
public class PublisherPool implements Closeable {

    private final ComponentLog logger;
    private final Map<String, Object> pulsarProducerProperties;
    private final PulsarClient pulsarClient;

    /** Idle leases per topic, ready to be handed out again. */
    private final Map<String, Queue<PooledPublisherLease>> idleLeases = new ConcurrentHashMap<>();

    /** Every lease created by this pool whose producer is still open, idle or in use. */
    private final Set<PooledPublisherLease> openLeases = ConcurrentHashMap.newKeySet();

    private volatile boolean closed = false;

    public PublisherPool(ComponentLog logger, Map<String, Object> pulsarProducerProperties, PulsarClient pulsarClient) {
        this.logger = logger;
        this.pulsarProducerProperties = pulsarProducerProperties;
        this.pulsarClient = pulsarClient;
    }

    /**
     * Returns a lease for the topic: an idle one if available, otherwise a newly created producer.
     *
     * @param topicName the topic to publish to
     * @return the lease, or {@code null} when the topic is blank or the producer cannot be created
     * @throws IllegalStateException if the pool has been closed
     */
    public PublisherLease obtainPublisher(String topicName) {
        if (isClosed()) {
            throw new IllegalStateException("Connection Pool is closed");
        }

        if (StringUtils.isBlank(topicName)) {
            return null;
        }

        final PooledPublisherLease idle = idleLeasesFor(topicName).poll();
        if (idle != null) {
            return idle;
        }

        try {
            return createLease(topicName);
        } catch (PulsarClientException pcEx) {
            logger.error("Unable to create producer", pcEx);
            return null;
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
        final Producer producer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(topicName)
                .loadConf(properties)
                .create();

        final PooledPublisherLease lease = new PooledPublisherLease(producer, topicName);
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

        private PooledPublisherLease(final Producer producer, final String topicName) {
            super(producer, logger);
            this.topicName = topicName;
        }

        @Override
        public void close() {
            if (PublisherPool.this.isClosed()) {
                closeProducer();
            } else {
                // return the producer to the pool; PublisherPool.close() closes it later because it stays in openLeases
                idleLeasesFor(topicName).offer(this);
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
