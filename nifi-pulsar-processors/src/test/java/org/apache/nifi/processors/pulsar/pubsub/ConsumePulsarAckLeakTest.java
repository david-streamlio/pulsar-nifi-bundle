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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for issue #53. Acknowledgements in async mode are submitted to an
 * ExecutorCompletionService, which retains the Future of every completed task until it is taken. Nothing
 * took them, so the queue grew by one Future per acknowledgement for the lifetime of the processor.
 * <p>
 * The test measures the leak behaviourally: after running the processor it counts how many completed
 * acknowledgement Futures are still sitting in the completion service. With the drain in place that count
 * stays near zero (only acks still in flight); without it, it grows with the number of triggers.
 */
public class ConsumePulsarAckLeakTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/events";
    private static final int TRIGGERS = 40;

    /**
     * How many retained acknowledgements still count as "in flight" rather than leaked.
     * <p>
     * The drain collects Futures that have already completed, so acknowledgements outstanding when the
     * last trigger returns are legitimately still queued. That number is NOT the ack pool's thread count:
     * the pool is a fixed thread pool with an unbounded work queue, so an arbitrary number of acks can be
     * submitted and not yet run. It depends on scheduling, and it goes up under load - which is exactly
     * how an earlier version of this test, asserting a hard 3, failed in CI with 8.
     * <p>
     * What is actually invariant is that the count does not scale with how long the processor ran: the
     * leak produced exactly one Future per acknowledgement, so it tracked the trigger count 1:1. This
     * allowance is a quarter of the triggers, which leaves a 4x margin below the leak while tolerating
     * scheduling noise. {@link #retainedAcksDoNotGrowWithTriggerCount()} tests the invariant directly.
     */
    private static int inFlightAllowance(final int triggers) {
        return Math.max(4, triggers / 4);
    }

    /** Exposes the ack completion service, which is protected on AbstractPulsarConsumerProcessor. */
    public static class AckProbeConsumePulsar extends ConsumePulsar {
        /** Drains and counts the acknowledgement Futures the processor left behind. */
        int countRetainedAcks() throws InterruptedException {
            if (getAckService() == null) {
                return 0;
            }

            int retained = 0;
            // a generous first wait lets any in-flight ack land, so we do not undercount the leak
            Future<Object> ack = getAckService().poll(2, TimeUnit.SECONDS);

            while (ack != null) {
                retained++;
                ack = getAckService().poll(100, TimeUnit.MILLISECONDS);
            }

            return retained;
        }
    }

    private AckProbeConsumePulsar processor;

    @Before
    public void init() throws InitializationException {
        processor = new AckProbeConsumePulsar();
        runner = TestRunners.newTestRunner(processor);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "true");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
    }

    /** Shared subscriptions acknowledge every message individually - the worst case for the leak. */
    @Test
    public void sharedSubscriptionAcksAreNotRetained() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        mockClientService.setMockMessageQueue(messages(TRIGGERS));

        // do not stop the processor: @OnUnscheduled tears the pools down, which would hide the leak
        runner.run(TRIGGERS, false);

        final int retained = processor.countRetainedAcks();
        assertTrue("Acknowledgement Futures are being retained: " + retained + " left after " + TRIGGERS
                + " triggers. The leak produced one per acknowledgement; anything near the trigger count "
                + "is that leak, not acks in flight (see issue #53)",
                retained <= inFlightAllowance(TRIGGERS));
    }

    /** Exclusive subscriptions acknowledge cumulatively, once per batch. */
    @Test
    public void exclusiveSubscriptionAcksAreNotRetained() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        mockClientService.setMockMessageQueue(messages(TRIGGERS));

        runner.run(TRIGGERS, false);

        final int retained = processor.countRetainedAcks();
        assertTrue("Acknowledgement Futures are being retained: " + retained + " left after " + TRIGGERS
                + " triggers. The leak produced one per acknowledgement; anything near the trigger count "
                + "is that leak, not acks in flight (see issue #53)",
                retained <= inFlightAllowance(TRIGGERS));
    }

    /**
     * The property that actually separates "a few acks still in flight" from "a leak": the retained count
     * must not scale with how long the processor has been running.
     * <p>
     * Measured at two scales in one test rather than against a fixed number, because the in-flight count
     * depends on scheduling and rises under load. With the bug the queue grew one Future per
     * acknowledgement, so quadrupling the triggers quadrupled the count - 40 and 160. Bounded, the two
     * measurements stay in the same range no matter how far apart the trigger counts are.
     */
    @Test
    public void retainedAcksDoNotGrowWithTriggerCount() throws Exception {
        final int fewTriggers = TRIGGERS;
        final int manyTriggers = TRIGGERS * 4;

        final int afterFew = retainedAfter(fewTriggers);
        final int afterMany = retainedAfter(manyTriggers);

        assertTrue("Retained acknowledgements track the trigger count: " + afterMany + " left after "
                + manyTriggers + " triggers, which is the one-Future-per-acknowledgement leak rather than "
                + "acks in flight (see issue #53)", afterMany <= inFlightAllowance(manyTriggers));

        // 4x the triggers must not mean anything like 4x the retained futures
        assertTrue("Retained acknowledgements scaled with the trigger count: " + afterFew + " after "
                + fewTriggers + " triggers but " + afterMany + " after " + manyTriggers
                + " (see issue #53)", afterMany < afterFew + (manyTriggers - fewTriggers) / 4);
    }

    /** Runs a fresh processor for the given number of triggers and returns what it left queued. */
    private int retainedAfter(final int triggers) throws Exception {
        processor = new AckProbeConsumePulsar();
        runner = TestRunners.newTestRunner(processor);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "true");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        mockClientService.setMockMessageQueue(messages(triggers));

        runner.run(triggers, false);

        return processor.countRetainedAcks();
    }

    /**
     * The aggravating case: an idle topic. The cumulative-ack task used to be submitted outside the
     * "did we receive anything?" guard, so every trigger queued a Future holding an
     * IndexOutOfBoundsException from messages.get(-1) - an idle processor leaked fastest of all.
     */
    @Test
    public void idleTopicDoesNotQueueFailedAcks() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        mockClientService.setMockMessageQueue(new ArrayList<>());

        runner.run(TRIGGERS, false);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        final int retained = processor.countRetainedAcks();
        assertTrue("An idle topic queued " + retained + " acknowledgement Futures over " + TRIGGERS
                + " triggers; it should queue none (see issue #53)", retained == 0);
    }

    private static List<Message<GenericRecord>> messages(final int count) {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();
        for (int n = 1; n <= count; n++) {
            msgs.add(new MockPulsarMessage<GenericRecord>(TOPIC, ("message-" + n).getBytes(UTF_8),
                    "1234:" + n + ":0", null, null));
        }
        return msgs;
    }
}
