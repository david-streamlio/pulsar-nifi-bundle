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
import java.util.concurrent.LinkedBlockingQueue;

public class PublisherPool implements Closeable {

    private final ComponentLog logger;
    private final Map<String, Object> pulsarProducerProperties;

    private final PulsarClient pulsarClient;

    private final BlockingQueue<PublisherLease> publisherQueue;

    private volatile boolean closed = false;

    public PublisherPool(ComponentLog logger, Map<String, Object> pulsarProducerProperties, PulsarClient pulsarClient) {
        this.logger = logger;
        this.pulsarProducerProperties = pulsarProducerProperties;
        this.pulsarClient = pulsarClient;
        this.publisherQueue = new LinkedBlockingQueue<>();
    }

    public PublisherLease obtainPublisher(String topicName) {
        if (isClosed()) {
            throw new IllegalStateException("Connection Pool is closed");
        }

        PublisherLease lease = null;

        try {
            lease = createLease(topicName);
        } catch (PulsarClientException pcEx) {
           logger.error("Unable to create producer", pcEx);
        }

        return lease;
    }

    private PublisherLease createLease(String topicName) throws PulsarClientException {
        if (StringUtils.isBlank(topicName)) {
            return null;
        }
        
        final Map<String, Object> properties = new HashMap<>(pulsarProducerProperties);
        Producer producer = pulsarClient.newProducer()
                .topic(topicName)
                .loadConf(properties)
                .create();

        final PublisherLease lease = new PublisherLease(producer, logger) {
            private volatile boolean closed = false;

            @Override
            public void close() {
                if (isClosed()) {
                    if (closed) {
                        return;
                    }

                    closed = true;
                    super.close();
                } else {
                    publisherQueue.remove(this);
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

        PublisherLease lease;
        while ((lease = publisherQueue.poll()) != null) {
            lease.close();
        }
    }
}
