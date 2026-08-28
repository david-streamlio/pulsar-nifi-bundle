/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class PublisherPool implements Closeable {

    private final ComponentLog logger;
    private final Map<String, Object> pulsarProducerProperties;

    private final PulsarClient pulsarClient;

    /**
     * Idle leases, keyed by topic. A lease wraps a producer bound to one topic, so a single shared queue
     * could hand a caller a producer for the wrong topic - which is why the queue has to be per topic.
     */
    private final Map<String, BlockingQueue<PublisherLease>> publisherQueues;

    private volatile boolean closed = false;

    public PublisherPool(ComponentLog logger, Map<String, Object> pulsarProducerProperties, PulsarClient pulsarClient) {
        this.logger = logger;
        this.pulsarProducerProperties = pulsarProducerProperties;
        this.pulsarClient = pulsarClient;
        this.publisherQueues = new ConcurrentHashMap<>();
    }

    public PublisherLease obtainPublisher(String topicName) {
        if (isClosed()) {
            throw new IllegalStateException("Connection Pool is closed");
        }

        if (StringUtils.isBlank(topicName)) {
            return null;
        }

        final PublisherLease pooled = queueFor(topicName).poll();

        if (pooled != null) {
            // the counter is cumulative per lease, so clear it before the next FlowFile uses it
            pooled.reset();
            return pooled;
        }

        try {
            return createLease(topicName);
        } catch (PulsarClientException pcEx) {
            logger.error("Unable to create producer", pcEx);
            return null;
        }
    }

    private BlockingQueue<PublisherLease> queueFor(final String topicName) {
        return publisherQueues.computeIfAbsent(topicName, t -> new LinkedBlockingQueue<>());
    }

    private PublisherLease createLease(final String topicName) throws PulsarClientException {
        final Map<String, Object> properties = new HashMap<>(pulsarProducerProperties);
        Producer producer = pulsarClient.newProducer()
                .topic(topicName)
                .loadConf(properties)
                .create();

        final PublisherLease lease = new PublisherLease(producer, logger) {
            private volatile boolean closed = false;

            @Override
            public void close() {
                if (closed) {
                    return;
                }

                if (isClosed()) {
                    // the pool is gone, so this really is the end of the producer's life
                    closed = true;
                    super.close();
                } else {
                    // hand it back for the next FlowFile on this topic rather than dropping it. This used
                    // to be publisherQueue.remove(this) on a queue nothing ever added to, so the producer
                    // was never returned and never closed - it simply leaked.
                    reset();
                    if (!queueFor(topicName).offer(this)) {
                        closed = true;
                        super.close();
                    }
                }
            }
        };

        return lease;
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() {
        closed = true;

        for (final BlockingQueue<PublisherLease> queue : publisherQueues.values()) {
            PublisherLease lease;
            while ((lease = queue.poll()) != null) {
                // the pool is closed now, so this closes the underlying producer
                lease.close();
            }
        }

        publisherQueues.clear();
    }
}
