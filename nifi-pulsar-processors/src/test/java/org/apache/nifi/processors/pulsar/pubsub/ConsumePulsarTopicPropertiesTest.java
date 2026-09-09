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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionMode;
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

    /**
     * The defaults have to leave every existing flow exactly as it was, which means matching the client's
     * own defaults in {@code ConsumerConfigurationData}. Asserted rather than assumed: a later change to any
     * of these four would silently alter the behaviour of every flow that never sets them, and validity
     * alone would not notice.
     */
    @Test
    public void theDefaultsAreTheClientDefaultsAndStayValid() {
        assertEquals(SubscriptionMode.Durable.name(),
                AbstractPulsarConsumerProcessor.SUBSCRIPTION_MODE.getDefaultValue());
        assertEquals("false", AbstractPulsarConsumerProcessor.READ_COMPACTED.getDefaultValue());
        assertEquals(RegexSubscriptionMode.PersistentOnly.name(),
                AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE.getDefaultValue());
        assertEquals("60 sec",
                AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD.getDefaultValue());

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

    /**
     * ...and the reason is stated once, rather than each half pointing at the subscription type the other
     * half forbids. Switching type in response to one message just produces the other, so a user following
     * the guidance goes in a circle - the messages are the only place they look.
     */
    @Test
    public void theConflictBetweenThemIsReportedAsOneReason() {
        for (final String type : new String[] {"Shared", "Key_Shared", "Exclusive", "Failover"}) {
            runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");
            runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "5");
            runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, type);

            final List<String> reasons = ((MockProcessContext) runner.getProcessContext()).validate().stream()
                    .filter(result -> !result.isValid())
                    .map(ValidationResult::getExplanation)
                    .collect(Collectors.toList());

            assertTrue("on " + type + " the conflict should be reported as its own reason, naming both "
                            + "properties, but the reasons were " + reasons,
                    reasons.stream().anyMatch(reason -> reason.contains("cannot both be set")));

            // The point of "one reason": no message may name a Subscription Type to move to, because every
            // such message contradicts the one above. Half one sends the user to Exclusive, the dead letter
            // rule sends them back to Shared, and alternating never reaches a valid state.
            assertTrue("on " + type + " no reason may direct the user at a Subscription Type while both are "
                            + "set, but the reasons were " + reasons,
                    reasons.stream().noneMatch(reason -> reason.contains("but the Subscription Type is")));
        }
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
     * The value reaches the client as whole seconds, so any fraction is silently discarded - not only a
     * sub-second value. Rejecting only the sub-second case would leave the same silent rounding one step up.
     */
    @Test
    public void aDiscoveryIntervalThatIsNotWholeSecondsIsRejected() {
        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN, "persistent://public/default/tp-.*");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");

        for (final String interval : new String[] {"500 millis", "0 sec", "1500 millis", "2500 millis"}) {
            runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, interval);
            runner.assertNotValid();
        }
    }

    @Test
    public void aWholeSecondDiscoveryIntervalIsValid() {
        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN, "persistent://public/default/tp-.*");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");

        for (final String interval : new String[] {"1 sec", "60 sec", "2 min"}) {
            runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, interval);
            runner.assertValid();
        }
    }

    /**
     * The property is inert with a topic list - the client reads it only on the pattern path - so failing a
     * topic-list flow over its value would reject a configuration it has no effect on. The inertness is
     * documented on the property and asserted for a valid value in
     * {@link #theTopicsPatternPropertiesAreInertWithATopicList()}; this pins it for an invalid one.
     */
    @Test
    public void aBadDiscoveryIntervalIsIgnoredWithATopicList() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, "500 millis");

        runner.assertValid();
    }

    /**
     * The one value a topic-list flow cannot ignore. The granularity rule is gated on Topics Pattern because
     * the client reads the value only on the pattern path, but
     * {@code ConsumerBuilderImpl.patternAutoDiscoveryPeriod} checks "interval needs to be >= 0" when the
     * consumer is built, for every consumer - and the builder is handed
     * {@code asTimePeriod(SECONDS).intValue()}, which overflows negative above the int range. So an
     * unbounded value is the only way a topic-list flow can be failed by this property, and it fails at
     * every schedule rather than on the canvas. 30000 days is 2,592,000,000 seconds, which is
     * -1,702,967,296 as an int.
     */
    @Test
    public void aDiscoveryIntervalAboveTheIntRangeIsRejectedEvenWithATopicList() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, "30000 days");

        runner.assertNotValid();
    }

    /**
     * The boundary of the int seconds the client keeps: Integer.MAX_VALUE seconds is representable and one
     * second more is not. Guards the comparison against being written on the millisecond value, where both
     * of these would pass.
     */
    @Test
    public void theDiscoveryIntervalBoundaryIsIntegerMaxValueSeconds() {
        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN, "persistent://public/default/tp-.*");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");

        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD,
                Integer.MAX_VALUE + " sec");
        runner.assertValid();

        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD,
                (Integer.MAX_VALUE + 1L) + " sec");
        runner.assertNotValid();
    }

    /**
     * Read Compacted with the default Subscription Initial Position of Latest warns rather than failing
     * validation. On a live topic it does deliver - new messages arrive and are read compacted - so it is
     * unusual, not invalid, and rejecting it would fail a working flow. On an idle topic it delivers
     * nothing, because the compacted view is the topic's history and a subscription at the tail has none of
     * it, which is indistinguishable from a broken flow. Hence a warning, once per start.
     */
    @Test
    public void readCompactedAtTheLatestPositionWarnsRatherThanFailingValidation() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");
        // Subscription Initial Position is left at its default, which is Latest.

        runner.assertValid();
        runner.run(1, false, true);

        assertEquals("exactly one warning is expected at scheduling", 1,
                runner.getLogger().getWarnMessages().stream()
                        .filter(m -> m.getMsg().contains("Read Compacted is enabled"))
                        .count());
    }

    @Test
    public void readCompactedAtTheEarliestPositionDoesNotWarn() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");

        runner.run(1, false, true);

        assertEquals("no warning is expected when the position can see the compacted view", 0,
                runner.getLogger().getWarnMessages().stream()
                        .filter(m -> m.getMsg().contains("Read Compacted is enabled"))
                        .count());
    }

    /** The warning is about the combination, so it must not fire when the compacted read is off. */
    @Test
    public void theLatestPositionAloneDoesNotWarn() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "false");

        runner.run(1, false, true);

        assertEquals("no warning is expected without Read Compacted", 0,
                runner.getLogger().getWarnMessages().stream()
                        .filter(m -> m.getMsg().contains("Read Compacted is enabled"))
                        .count());
    }

    @Test
    public void aNonDurableSubscriptionIsValid() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_MODE, "NonDurable");

        runner.assertValid();
    }

    /**
     * A non-durable subscription leaves no cursor, so Earliest has nothing to resume from and re-reads the
     * topic from the start on every schedule - and on every eviction from the consumer cache. Valid, and
     * sometimes wanted, but the Read Compacted guidance sends users to Earliest, so the combination is easy
     * to arrive at without meaning to.
     */
    @Test
    public void aNonDurableSubscriptionAtTheEarliestPositionWarns() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_MODE, "NonDurable");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");

        runner.assertValid();
        runner.run(1, false, true);

        assertEquals("exactly one warning is expected at scheduling", 1,
                runner.getLogger().getWarnMessages().stream()
                        .filter(m -> m.getMsg().contains("no cursor for a non-durable subscription"))
                        .count());
    }

    @Test
    public void aDurableSubscriptionAtTheEarliestPositionDoesNotWarn() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");

        runner.run(1, false, true);

        assertEquals("no warning is expected when the cursor is durable", 0,
                runner.getLogger().getWarnMessages().stream()
                        .filter(m -> m.getMsg().contains("no cursor for a non-durable subscription"))
                        .count());
    }

    /** The warning is about the combination: tailing without a cursor is exactly what NonDurable is for. */
    @Test
    public void aNonDurableSubscriptionAtTheLatestPositionDoesNotWarn() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_MODE, "NonDurable");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Latest");

        runner.run(1, false, true);

        assertEquals("no warning is expected when tailing", 0,
                runner.getLogger().getWarnMessages().stream()
                        .filter(m -> m.getMsg().contains("no cursor for a non-durable subscription"))
                        .count());
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

    /**
     * Validation is not evidence that a property is applied. These four are the whole point of the change,
     * and every other test here stops at the canvas - so without this, deleting any of the four calls from
     * {@code getConsumerBuilder} leaves the suite green.
     */
    private void scheduleOnceWithAMessage() {
        @SuppressWarnings("unchecked")
        final Message<GenericRecord> message = mock(Message.class);
        when(message.getData()).thenReturn("mocked message".getBytes(StandardCharsets.UTF_8));
        mockClientService.setMockMessage(message);

        runner.run(1, true);
    }

    @Test
    public void readCompactedReachesTheConsumerBuilder() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");

        scheduleOnceWithAMessage();

        verify(mockClientService.getMockConsumerBuilder(), times(1)).readCompacted(true);
    }

    @Test
    public void subscriptionModeReachesTheConsumerBuilder() {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_MODE,
                SubscriptionMode.NonDurable.name());

        scheduleOnceWithAMessage();

        verify(mockClientService.getMockConsumerBuilder(), times(1))
                .subscriptionMode(SubscriptionMode.NonDurable);
    }

    @Test
    public void theTopicsPatternPropertiesReachTheConsumerBuilder() {
        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN, "persistent://public/default/tp-.*");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE,
                RegexSubscriptionMode.AllTopics.name());
        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, "5 sec");

        scheduleOnceWithAMessage();

        verify(mockClientService.getMockConsumerBuilder(), times(1))
                .subscriptionTopicsMode(RegexSubscriptionMode.AllTopics);
        verify(mockClientService.getMockConsumerBuilder(), times(1))
                .patternAutoDiscoveryPeriod(5, TimeUnit.SECONDS);
    }
}
