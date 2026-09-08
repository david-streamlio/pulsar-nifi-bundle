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

    /**
     * The client's precondition has two halves and its message states both: "exclusive or failover
     * PERSISTENT subscriptions". Enforcing only the subscription type leaves the exact failure this
     * validation exists to prevent - valid on the canvas, throwing on every schedule.
     */
    @Test
    public void readCompactedIsRejectedOnANonPersistentTopic() {
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "non-persistent://public/default/live");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertNotValid();
    }

    /** One non-persistent topic in a list is enough: the client requires every topic to be persistent. */
    @Test
    public void readCompactedIsRejectedWhenAnyTopicInTheListIsNonPersistent() {
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS,
                "persistent://public/default/a,non-persistent://public/default/b");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertNotValid();
    }

    /** A bare topic name is in the persistent domain by default, so it stays valid. */
    @Test
    public void readCompactedIsValidOnAnUnqualifiedTopicName() {
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "my-topic");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertValid();
    }

    /**
     * The case the client cannot catch. With a pattern its topic list is empty, so its persistent-domain
     * check passes vacuously and any non-persistent topic the pattern matches is subscribed and served as a
     * live stream - the flow reads a full stream while its configuration says compacted, with no error.
     */
    @Test
    public void readCompactedIsRejectedWithAPatternThatCanMatchNonPersistentTopics() {
        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN, "persistent://public/default/tp-.*");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        for (final String mode : new String[] {"NonPersistentOnly", "AllTopics"}) {
            runner.setProperty(AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE, mode);
            runner.assertNotValid();
        }

        runner.setProperty(AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE, "PersistentOnly");
        runner.assertValid();
    }

    /**
     * A pattern's own domain scheme is inert, so it must not be rejected. TopicsPatternFactory runs the
     * pattern through TopicList.removeTopicDomainScheme() and matching strips the scheme from every
     * candidate topic, so the domain comes from Topics Pattern Match Mode alone. An earlier version of
     * this validator rejected the prefix and failed this working configuration.
     */
    @Test
    public void aNonPersistentPatternPrefixIsInertAndMustNotBeRejected() {
        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN, "non-persistent://public/default/tp-.*");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE, "PersistentOnly");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        runner.assertValid();
    }

    /**
     * The interval is passed to the client in whole seconds and the client clamps 0 to 1, so a sub-second
     * value becomes a topic lookup every second rather than the interval that was asked for - silently.
     */
    @Test
    public void aSubSecondDiscoveryIntervalIsRejected() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");

        for (final String interval : new String[] {"500 millis", "0 sec"}) {
            runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, interval);
            runner.assertNotValid();
        }
    }

    @Test
    public void aWholeSecondDiscoveryIntervalIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, "1 sec");

        runner.assertValid();
    }

    /** None of the new domain rules may fire when the compacted read is off. */
    @Test
    public void aNonPersistentTopicIsValidWhenReadCompactedIsOff() {
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "non-persistent://public/default/live");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "false");

        runner.assertValid();
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
