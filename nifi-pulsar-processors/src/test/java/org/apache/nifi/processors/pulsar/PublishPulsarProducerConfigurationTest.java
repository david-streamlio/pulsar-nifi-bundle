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
package org.apache.nifi.processors.pulsar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.junit.Before;
import org.junit.Test;

/**
 * Every producer property must actually reach the producer.
 * <p>
 * The producer is configured by handing a {@code Map<String, Object>} to {@code ProducerBuilder.loadConf()},
 * so a property is wired only if someone remembered to put it in the map - a missing entry looks like nothing
 * at all. That is how #180 lost <i>Message Routing Mode</i> and <i>Max Pending Messages</i>: both had been
 * shown in the UI for two years while the producer ran with the client defaults.
 * <p>
 * These tests assert against the {@link ProducerConfigurationData} that {@code loadConf} actually produces,
 * not against the map. The distinction matters: {@code loadConf} serialises the map through JSON, so a value
 * of a type that does not survive that trip is dropped in silence. {@code batcherBuilder} is exactly such a
 * value, which is why it is set on the builder instead and asserted separately.
 */
public class PublishPulsarProducerConfigurationTest extends AbstractPulsarProcessorTest<byte[]> {

    private AbstractPulsarProducerProcessor<byte[]> processor;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "persistent://public/default/configuration");
        processor = (AbstractPulsarProducerProcessor<byte[]>) runner.getProcessor();
    }

    /**
     * The guard #186 asked for: whatever the map claims, this is what the client ends up configured with.
     * A property dropped from the map shows up here as a client default.
     */
    @Test
    public void everyConfiguredPropertyReachesTheProducerConfiguration() {
        runner.setProperty(AbstractPulsarProducerProcessor.SEND_TIMEOUT, "5 sec");
        runner.setProperty(AbstractPulsarProducerProcessor.ACCESS_MODE, "Exclusive");
        runner.setProperty(AbstractPulsarProducerProcessor.HASHING_SCHEME, "Murmur3_32Hash");
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_ROUTING_MODE, "RoundRobinPartition");
        runner.setProperty(AbstractPulsarProducerProcessor.PENDING_MAX_MESSAGES, "500");

        final ProducerConfigurationData conf = producerConfiguration();

        assertEquals(5000L, conf.getSendTimeoutMs());
        assertEquals(ProducerAccessMode.Exclusive, conf.getAccessMode());
        assertEquals(HashingScheme.Murmur3_32Hash, conf.getHashingScheme());
        assertEquals(500, conf.getMaxPendingMessages());
        // the two #180 lost, kept here so they cannot be lost the same way twice
        assertEquals("RoundRobinPartition", conf.getMessageRoutingMode().name());
    }

    /** The defaults the processor declares are the defaults the client is given. */
    @Test
    public void theDeclaredDefaultsReachTheProducerConfiguration() {
        final ProducerConfigurationData conf = producerConfiguration();

        assertEquals("Send Timeout defaults to 30 sec", 30000L, conf.getSendTimeoutMs());
        assertEquals(ProducerAccessMode.Shared, conf.getAccessMode());
        assertEquals(HashingScheme.JavaStringHash, conf.getHashingScheme());
    }

    /** A send timeout of 0 has a specific meaning to Pulsar - wait forever - so it must survive as 0. */
    @Test
    public void aZeroSendTimeoutIsPassedThroughRatherThanTreatedAsUnset() {
        runner.setProperty(AbstractPulsarProducerProcessor.SEND_TIMEOUT, "0 sec");

        assertEquals(0L, producerConfiguration().getSendTimeoutMs());
    }

    /**
     * The reason Batch Builder is not in the configuration map. Placing a BatcherBuilder there and letting
     * loadConf carry it leaves the producer on the default builder without an error - so this asserts the
     * property reaches the producer by the path that works, and that the map is not quietly relied on.
     */
    @Test
    public void theKeyBasedBatchBuilderIsSetOnTheBuilderNotThroughTheConfigurationMap() {
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHING_ENABLED, "true");
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHER_BUILDER, "Key based");

        assertEquals(BatcherBuilder.KEY_BASED, processor.getBatcherBuilder(runner.getProcessContext()));
        assertTrue("a BatcherBuilder in the configuration map would be dropped by loadConf without an error",
                !configurationMap().containsKey("batcherBuilder"));
    }

    @Test
    public void theDefaultBatchBuilderIsUsedWhenNoneIsChosen() {
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHING_ENABLED, "true");

        assertEquals(BatcherBuilder.DEFAULT, processor.getBatcherBuilder(runner.getProcessContext()));
    }

    /** With batching off the builder is meaningless, and the client is not given one. */
    @Test
    public void noBatchBuilderIsSetWhenBatchingIsDisabled() {
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHING_ENABLED, "false");
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHER_BUILDER, "Key based");

        assertNull(processor.getBatcherBuilder(runner.getProcessContext()));
    }

    private Map<String, Object> configurationMap() {
        final Map<String, Object> config = processor.getPulsarProducerConfiguration(runner.getProcessContext());
        assertNotNull(config);
        return config;
    }

    /** What {@code ProducerBuilder.loadConf()} actually makes of the map the processor builds. */
    private ProducerConfigurationData producerConfiguration() {
        return ConfigurationDataUtils.loadData(
                configurationMap(), new ProducerConfigurationData(), ProducerConfigurationData.class);
    }
}
