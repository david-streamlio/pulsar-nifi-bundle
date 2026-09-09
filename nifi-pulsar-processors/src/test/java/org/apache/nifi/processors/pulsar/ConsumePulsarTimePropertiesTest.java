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
 * The rule is the same for both: the value must be representable in the granularity the client stores it in,
 * or it is rejected at validation rather than applied as something else. <i>Expire Time of Incomplete Chunked
 * Message</i> is a long of whole milliseconds, with 0 meaning "never expire". <i>Auto Update Partition
 * Interval</i> is an int of whole seconds, and the client refuses zero, so the value must be a whole number of
 * seconds between 1 and {@link Integer#MAX_VALUE}.
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

    /**
     * Below a millisecond the same trap would reappear one unit down: {@code 0.5 millis} truncates to 0, and 0 is
     * "never expire" - the opposite of the shortest expiry the user asked for. Not a whole millisecond, so rejected.
     */
    @Test
    public void aChunkExpiryUnderOneMillisecondIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE, "0.5 millis");

        runner.assertNotValid();
    }

    @Test
    public void aChunkExpiryWithAFractionOfAMillisecondIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE, "1500000 nanos");

        runner.assertNotValid();
    }

    /** Zero is the documented "never expire", chosen deliberately rather than arrived at by truncation. */
    @Test
    public void aZeroChunkExpiryIsValidAndDisablesTheExpiry() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE, "0 sec");

        runner.assertValid();
        assertEquals(0L, consumerConfiguration().getExpireTimeOfIncompleteChunkedMessageMillis());
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
     * A second or more with a fraction is not representable: the client keeps whole seconds, so {@code 90500 millis}
     * would run as 90 s while the configuration says 90.5. Rejected, like the sibling Topics Pattern Discovery
     * Interval, rather than silently applied as something else.
     */
    @Test
    public void aFractionalPartitionUpdateIntervalIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "90500 millis");

        runner.assertNotValid();
    }

    /**
     * The client takes the seconds as an int, and the processor used {@code intValue()} on the long: past
     * {@link Integer#MAX_VALUE} seconds the value wrapped - negative and refused by the client on every trigger, or
     * positive and silently far shorter than asked ({@code 10000 weeks} ran as about 55 years, not 191).
     */
    @Test
    public void aPartitionUpdateIntervalPastTheIntRangeIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "30000 days");
        runner.assertNotValid();

        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "10000 weeks");
        runner.assertNotValid();

        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, (Integer.MAX_VALUE + 1L) + " sec");
        runner.assertNotValid();
    }

    /** The top of the range is representable and applied as given. */
    @Test
    public void thePartitionUpdateIntervalAtTheTopOfTheIntRangeIsValid() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, Integer.MAX_VALUE + " sec");

        runner.assertValid();
        assertEquals((long) Integer.MAX_VALUE, consumerConfiguration().getAutoUpdatePartitionsIntervalSeconds());
    }

    /** Whole seconds are applied as given. */
    @Test
    public void aWholeSecondPartitionUpdateIntervalIsAppliedAsGiven() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.AUTO_UPDATE_PARTITION_INTERVAL, "90 sec");

        runner.assertValid();
        assertEquals(90L, consumerConfiguration().getAutoUpdatePartitionsIntervalSeconds());
        assertEquals(0, runner.getLogger().getWarnMessages().size());
    }
}
