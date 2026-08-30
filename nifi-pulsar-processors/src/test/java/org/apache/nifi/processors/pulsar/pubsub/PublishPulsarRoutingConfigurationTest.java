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
package org.apache.nifi.processors.pulsar.pubsub;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * The producer has to be configured with the routing mode and the pending-message bound the user picked.
 * <p>
 * Both properties were applied on the ProducerBuilder until producer creation moved to
 * {@code PublisherPool.loadConf(map)}; they were not carried over into the map, so the producer ran with
 * the client defaults whatever the processor was configured with. The map handed to {@code loadConf()} is
 * what production actually applies, so that is what is asserted here.
 */
public class PublishPulsarRoutingConfigurationTest extends AbstractPulsarProcessorTest<byte[]> {

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "test-topic");
    }

    @Test
    public void theConfiguredRoutingModeAndPendingBoundReachTheProducer() {
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_ROUTING_MODE, "SinglePartition");
        runner.setProperty(AbstractPulsarProducerProcessor.PENDING_MAX_MESSAGES, "250");

        final Map<String, Object> config = producerConfiguration();

        assertEquals(MessageRoutingMode.SinglePartition, config.get("messageRoutingMode"));
        assertEquals(250, config.get("maxPendingMessages"));
    }

    @Test
    public void theDefaultsAreAppliedExplicitly() {
        final Map<String, Object> config = producerConfiguration();

        assertEquals(MessageRoutingMode.RoundRobinPartition, config.get("messageRoutingMode"));
        assertEquals(1000, config.get("maxPendingMessages"));
    }

    /** There is no way to hand the producer a MessageRouter, so the mode that needs one is refused up front. */
    @Test
    public void customPartitionIsRejectedAtValidation() {
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_ROUTING_MODE, "CustomPartition");

        runner.assertNotValid();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> producerConfiguration() {
        runner.enqueue("payload");
        runner.run();

        final ArgumentCaptor<Map<String, Object>> config = ArgumentCaptor.forClass(Map.class);
        verify(mockClientService.getMockProducerBuilder(), times(1)).loadConf(config.capture());
        return config.getValue();
    }
}
