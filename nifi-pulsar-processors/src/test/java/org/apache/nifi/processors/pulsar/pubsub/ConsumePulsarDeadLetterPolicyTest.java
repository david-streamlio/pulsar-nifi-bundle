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

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * The dead letter policy is configuration the broker can silently ignore, so the processor rejects the
 * combinations where it would never take effect.
 * <p>
 * Pulsar builds a dead letter policy only for Shared and Key_Shared subscriptions. On an Exclusive or
 * Failover subscription the consumer is accepted and simply never dead-letters anything - a flow would sit
 * watching a dead letter topic that cannot receive a message, with nothing anywhere saying why. That is the
 * same failure shape as {@code CustomPartition} on the producer, which 2.10.0 moved to validation for the
 * same reason.
 */
public class ConsumePulsarDeadLetterPolicyTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/dead-letter";

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
    }

    /** No dead letter policy at all is the default, and stays valid: the broker redelivers indefinitely. */
    @Test
    public void aConsumerWithoutADeadLetterPolicyIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");

        runner.assertValid();
    }

    @Test
    public void aDeadLetterPolicyIsValidOnASharedSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "5");

        runner.assertValid();
    }

    @Test
    public void aDeadLetterPolicyIsValidOnAKeySharedSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Key_Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "5");

        runner.assertValid();
    }

    /** The client ignores the policy here, so accepting the configuration would be a lie. */
    @Test
    public void aDeadLetterPolicyIsRejectedOnAnExclusiveSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "5");

        runner.assertNotValid();
    }

    @Test
    public void aDeadLetterPolicyIsRejectedOnAFailoverSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Failover");
        runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "5");

        runner.assertNotValid();
    }

    /**
     * A dead letter topic without a redelivery count names a destination nothing can ever be sent to:
     * the count is what arms the policy.
     */
    @Test
    public void aDeadLetterTopicWithoutARedeliveryCountIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.DEAD_LETTER_TOPIC, TOPIC + "-DLQ");

        runner.assertNotValid();
    }

    @Test
    public void aDeadLetterTopicWithARedeliveryCountIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "5");
        runner.setProperty(AbstractPulsarConsumerProcessor.DEAD_LETTER_TOPIC, TOPIC + "-DLQ");

        runner.assertValid();
    }
}
