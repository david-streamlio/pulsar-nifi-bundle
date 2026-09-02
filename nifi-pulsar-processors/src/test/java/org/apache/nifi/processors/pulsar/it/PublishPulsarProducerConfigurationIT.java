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
package org.apache.nifi.processors.pulsar.it;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.Before;
import org.junit.Test;

/**
 * Producer configuration that only a broker can confirm.
 * <p>
 * {@code PublishPulsarProducerConfigurationTest} asserts what the client is configured with, which catches a
 * property dropped from the configuration map. It cannot show that the broker then honours it. Producer
 * access mode is the one of the four with a plainly observable broker behaviour - the broker refuses a second
 * producer - so it is what pins the configuration actually taking effect end to end.
 */
public class PublishPulsarProducerConfigurationIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    /**
     * With Exclusive access the processor's producer owns the topic, and a second producer is refused. That
     * refusal is the whole point of the property: it is how two flows are stopped from writing one topic.
     */
    @Test
    public void anExclusiveProducerLocksOutEveryOtherProducer() throws Exception {
        final String topic = topic("exclusive");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.ACCESS_MODE, ProducerAccessMode.Exclusive.name());

        runner.enqueue("owned");
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);

        // The processor's producer is still open and holds the topic exclusively.
        try (Producer<byte[]> intruder = getClient().newProducer(Schema.BYTES).topic(topic).create()) {
            fail("a second producer was allowed onto a topic held with Exclusive access: " + intruder);
        } catch (final PulsarClientException.ProducerBusyException expected) {
            assertTrue(true);
        }
    }

    /**
     * The default. Shared access has to keep letting other producers in, or the new property would be a
     * breaking change for every existing flow.
     */
    @Test
    public void aSharedProducerLeavesTheTopicOpenToOthers() throws Exception {
        final String topic = topic("shared");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        // deliberately not setting ACCESS_MODE: this is what an existing flow looks like

        runner.enqueue("shared-write");
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);

        try (Producer<byte[]> second = getClient().newProducer(Schema.BYTES).topic(topic).create()) {
            second.send("also-mine".getBytes());
        }
    }

    private static String topic(final String name) {
        return "persistent://public/default/producer-config-" + name + "-" + System.nanoTime();
    }
}
