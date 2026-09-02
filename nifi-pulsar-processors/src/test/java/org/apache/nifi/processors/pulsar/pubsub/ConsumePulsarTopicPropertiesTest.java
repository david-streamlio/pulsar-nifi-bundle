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
 * Read Compacted, Subscription Mode, and the two properties that finish Topics Pattern.
 * <p>
 * Read Compacted is the one with a constraint the processor has to enforce: the client refuses it at
 * subscribe time with "Read compacted can only be used with exclusive or failover persistent subscriptions",
 * so a flow that configures it on a Shared subscription validates cleanly and then fails every time it is
 * scheduled. That constraint is the mirror of the dead letter policy's - a compacted read needs a single
 * active consumer, a dead letter policy needs competing ones - so the two can never both be on.
 */
public class ConsumePulsarTopicPropertiesTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/topic-properties";

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
    }

    /** The defaults have to leave every existing flow exactly as it was. */
    @Test
    public void theDefaultsAreTheClientDefaultsAndStayValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");

        runner.assertValid();
    }

    @Test
    public void readCompactedIsValidOnAnExclusiveSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertValid();
    }

    @Test
    public void readCompactedIsValidOnAFailoverSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Failover");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertValid();
    }

    /** Shared is the processor's default subscription type, so this is the combination most likely hit. */
    @Test
    public void readCompactedIsRejectedOnASharedSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertNotValid();
    }

    @Test
    public void readCompactedIsRejectedOnAKeySharedSubscription() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Key_Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertNotValid();
    }

    /**
     * The two constraints are mirror images, so no subscription type satisfies both. Pinned because it
     * would otherwise be discovered by a user who configured both and could not see why neither worked.
     */
    @Test
    public void readCompactedAndADeadLetterPolicyCannotBothBeEnabled() {
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");
        runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "5");

        // Shared satisfies the dead letter policy and breaks the compacted read
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.assertNotValid();

        // Exclusive satisfies the compacted read and breaks the dead letter policy
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.assertNotValid();
    }

    /** Turning it off must not carry the constraint with it. */
    @Test
    public void readCompactedFalseIsValidOnEverySubscriptionType() {
        for (final String type : new String[] {"Exclusive", "Failover", "Shared", "Key_Shared"}) {
            runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, type);
            runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "false");

            runner.assertValid();
        }
    }

    @Test
    public void aNonDurableSubscriptionIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_MODE, "NonDurable");

        runner.assertValid();
    }

    /** The two Topics Pattern properties are accepted alongside a pattern subscription. */
    @Test
    public void theTopicsPatternPropertiesAreValidWithAPattern() {
        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN, "persistent://public/default/tp-.*");
        runner.setProperty(AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE, "AllTopics");
        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, "5 sec");

        runner.assertValid();
    }

    /**
     * They are inert with a topic list rather than rejected. Unlike a dead letter topic that can never
     * receive a message, a match mode on a subscription that does no matching misleads nobody, and
     * rejecting it would make the two properties awkward to leave configured while switching between a
     * list and a pattern.
     */
    @Test
    public void theTopicsPatternPropertiesAreInertWithATopicList() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE, "AllTopics");
        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, "5 sec");

        runner.assertValid();
    }
}
