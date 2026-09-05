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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * A negatively acknowledged message is redelivered by <i>Negative Acknowledgment Redelivery Delay</i> and by
 * nothing else: the Pulsar client drops it from the acknowledgment-timeout tracker the moment it is nacked
 * ({@code ConsumerImpl.negativeAcknowledge}). So the delay is the whole story for a message the processor could
 * not write, and <i>Acknowledgment Timeout</i> is not a ceiling for it.
 * <p>
 * Two things follow, and both are pinned here. The default delay has to be shorter than the shortest
 * <i>Acknowledgment Timeout</i> the validator accepts, or a default flow waits longer after a write failure
 * than it did before negative acknowledgement existed (#218). And a configured delay longer than the timeout
 * is rejected, so a tuned flow cannot reintroduce the same inversion.
 */
public class ConsumePulsarNegativeAckDelayTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/nack-delay";

    /** The floor {@code customValidate} enforces on Acknowledgment Timeout, in seconds. */
    private static final long ACK_TIMEOUT_FLOOR_SECONDS = 10;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
    }

    /**
     * The invariant behind the default: whatever Acknowledgment Timeout a valid flow has, a nacked message
     * comes back sooner than an unacknowledged one would have. That holds only if the default delay is
     * below the timeout's floor.
     */
    @Test
    public void theDefaultDelayIsShorterThanTheShortestAllowedAcknowledgmentTimeout() {
        final long defaultDelayMillis = FormatUtils.getTimeDuration(
                AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY.getDefaultValue(), TimeUnit.MILLISECONDS);

        assertTrue("the default Negative Acknowledgment Redelivery Delay (" + defaultDelayMillis + " ms) is not shorter "
                        + "than the " + ACK_TIMEOUT_FLOOR_SECONDS + " s floor of Acknowledgment Timeout, so a write "
                        + "failure would stall longer than the timeout it was meant to beat",
                defaultDelayMillis < TimeUnit.SECONDS.toMillis(ACK_TIMEOUT_FLOOR_SECONDS));
    }

    /** Defaults only, which is what every flow created before the property existed looks like. */
    @Test
    public void theDefaultsAreValid() {
        runner.assertValid();
    }

    @Test
    public void aDelayShorterThanTheAcknowledgmentTimeoutIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "30 sec");
        runner.setProperty(AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY, "5 sec");

        runner.assertValid();
    }

    /** Equal is allowed: the nack then buys nothing, but it takes nothing away either. */
    @Test
    public void aDelayEqualToTheAcknowledgmentTimeoutIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "30 sec");
        runner.setProperty(AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY, "30 sec");

        runner.assertValid();
    }

    /**
     * Longer than the timeout is the inversion: the timeout no longer applies to a nacked message, so this
     * configuration makes a write failure wait longer than a plain rollback would have.
     */
    @Test
    public void aDelayLongerThanTheAcknowledgmentTimeoutIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "30 sec");
        runner.setProperty(AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY, "1 min");

        runner.assertNotValid();
    }

    /** The rule compares durations, not the strings: "30 sec" and "30000 millis" are the same timeout. */
    @Test
    public void theComparisonIsOnDurationsNotOnTheWrittenValues() {
        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "30000 millis");
        runner.setProperty(AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY, "31 sec");

        runner.assertNotValid();
    }
}
