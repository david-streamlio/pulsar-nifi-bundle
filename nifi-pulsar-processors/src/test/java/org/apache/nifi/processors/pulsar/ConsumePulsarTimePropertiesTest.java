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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.pulsar.StandardPulsarClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Two consumer time properties reach the Pulsar client through a unit conversion, and what the client ends up
 * configured with is not always what was typed (#225). These tests build the consumer configuration through a
 * real {@code PulsarClient} - one that never connects; the client is lazy about that - and assert against the
 * {@link ConsumerConfigurationData} the builder holds, which is the value the consumer would run with.
 * <p>
 * <i>Expire Time of Incomplete Chunked Message</i> is stored by the client in milliseconds, so every value can
 * be honoured exactly. <i>Auto Update Partition Interval</i> is stored in whole seconds and the client refuses
 * zero, so a value under a second cannot be applied at all and a fraction of a second above that is dropped;
 * the first is rejected at validation and the second is warned about when the processor is scheduled.
 */
public class ConsumePulsarTimePropertiesTest {

    private TestRunner runner;
    private StandardPulsarClientService clientService;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        // A real client against a broker that does not exist: building it opens no connection, and nothing
        // here subscribes, so the configuration can be read without a broker.
        clientService = new StandardPulsarClientService();
        runner.addControllerService("pulsar-client", clientService);
        runner.setProperty(clientService, StandardPulsarClientService.PULSAR_SERVICE_URL, "pulsar://localhost:1");
        runner.enableControllerService(clientService);
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "persistent://public/default/time-properties");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
    }

    @After
    public void closeClient() {
        runner.disableControllerService(clientService);
    }

    /** The configuration the consumer would be created with, read from the builder the processor prepares. */
    @SuppressWarnings("unchecked")
    private ConsumerConfigurationData<GenericRecord> consumerConfiguration() throws PulsarClientException {
        final AbstractPulsarConsumerProcessor<GenericRecord> processor =
                (AbstractPulsarConsumerProcessor<GenericRecord>) runner.getProcessor();
        processor.init(runner.getProcessContext());
        return ((ConsumerBuilderImpl<GenericRecord>) processor.getConsumerBuilder(runner.getProcessContext())).getConf();
    }

    // --- Expire Time of Incomplete Chunked Message ---------------------------------------------------------

    /** The client keeps milliseconds, so there is no reason to lose the fraction on the way. */
    @Test
    public void theChunkExpiryKeepsItsFractionOfASecond() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE, "1500 millis");

        assertEquals(1500L, consumerConfiguration().getExpireTimeOfIncompleteChunkedMessageMillis());
    }

    /**
     * The case that mattered most: converted to whole seconds, a sub-second value became 0, and the client
     * schedules chunk expiry only for a value above 0 - so "expire quickly" silently meant "never expire".
     */
    @Test
    public void aSubSecondChunkExpiryIsAppliedRatherThanDisablingExpiry() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE, "500 millis");

        assertEquals(500L, consumerConfiguration().getExpireTimeOfIncompleteChunkedMessageMillis());
    }

    @Test
    public void theDefaultChunkExpiryIsOneMinute() throws Exception {
        assertEquals(TimeUnit.MINUTES.toMillis(1), consumerConfiguration().getExpireTimeOfIncompleteChunkedMessageMillis());
    }

    // --- Auto Update Partition Interval ------------------------------------------------------------------

    /**
     * Below a second the client cannot be given the value at all: it refuses zero. Before the rule the processor
     * validated and then failed on every trigger with the client's message about an interval nobody typed.
     */
    @Test
    public void aPartitionUpdateIntervalUnderOneSecondIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "500 millis");

        runner.assertNotValid();
    }

    @Test
    public void aPartitionUpdateIntervalOfExactlyOneSecondIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "1000 millis");

        runner.assertValid();
    }

    /** The reason for the floor, pinned against the client itself so a client that relaxes it is noticed. */
    @Test
    public void theClientRefusesAZeroSecondPartitionUpdateInterval() {
        try {
            clientService.getPulsarClient().newConsumer(Schema.BYTES).autoUpdatePartitionsInterval(0, TimeUnit.SECONDS);
            fail("the client accepted a zero-second partition update interval; the validation floor may no longer be needed");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("> 0"));
        }
    }

    /**
     * A second or more with a fraction runs, only coarser than asked: the client keeps whole seconds. That is
     * allowed - it worked before and works now - but the processor says what it applied, once per start.
     */
    @Test
    public void aFractionalPartitionUpdateIntervalIsAppliedAsWholeSecondsAndWarnsWhenScheduled() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "90500 millis");

        runner.assertValid();
        assertEquals(90L, consumerConfiguration().getAutoUpdatePartitionsIntervalSeconds());

        assertEquals("one warning about the dropped fraction, got " + runner.getLogger().getWarnMessages(),
                1, runner.getLogger().getWarnMessages().size());
        final LogMessage warning = runner.getLogger().getWarnMessages().get(0);
        final String rendered = String.format(warning.getMsg().replace("{}", "%s"), warning.getArgs());
        assertTrue(rendered, rendered.contains("Auto Update Partition Interval") && rendered.contains("90500 millis")
                && rendered.contains("90 seconds"));
    }

    /** Whole seconds are applied as given, and nothing is logged about them. */
    @Test
    public void aWholeSecondPartitionUpdateIntervalIsAppliedAsGivenWithoutAWarning() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "90 sec");

        assertEquals(90L, consumerConfiguration().getAutoUpdatePartitionsIntervalSeconds());
        assertEquals(0, runner.getLogger().getWarnMessages().size());
    }
}
