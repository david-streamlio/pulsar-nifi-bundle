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
     * The drain collects acknowledgement Futures that have already completed, so the ones still in flight
     * when the last trigger returns are legitimately still queued. The ack pool is
     * {@code newFixedThreadPool(MAX_ASYNC_REQUESTS + 1)} and MAX_ASYNC_REQUESTS defaults to 2, so at most
     * this many acknowledgements can be in flight at once. What matters is that the number is bounded by
     * the pool rather than growing with the number of triggers - see {@link #retainedAcksDoNotGrowWithTriggerCount()}.
     */
    private static final int MAX_IN_FLIGHT_ACKS = 3;

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
                + " triggers, more than the " + MAX_IN_FLIGHT_ACKS + " that can be in flight (see issue #53)",
                retained <= MAX_IN_FLIGHT_ACKS);
    }

    /** Exclusive subscriptions acknowledge cumulatively, once per batch. */
    @Test
    public void exclusiveSubscriptionAcksAreNotRetained() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        mockClientService.setMockMessageQueue(messages(TRIGGERS));

        runner.run(TRIGGERS, false);

        final int retained = processor.countRetainedAcks();
        assertTrue("Acknowledgement Futures are being retained: " + retained + " left after " + TRIGGERS
                + " triggers, more than the " + MAX_IN_FLIGHT_ACKS + " that can be in flight (see issue #53)",
                retained <= MAX_IN_FLIGHT_ACKS);
    }

    /**
     * The property that actually separates "a few acks still in flight" from "a leak": the retained count
     * must be bounded by the ack pool, not proportional to how long the processor has been running. With
     * the bug this grew one Future per acknowledgement, so quadrupling the triggers quadrupled the count.
     */
    @Test
    public void retainedAcksDoNotGrowWithTriggerCount() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        final int manyTriggers = TRIGGERS * 4;
        mockClientService.setMockMessageQueue(messages(manyTriggers));

        runner.run(manyTriggers, false);

        final int retained = processor.countRetainedAcks();
        assertTrue("Retained acknowledgements scale with the trigger count: " + retained + " left after "
                + manyTriggers + " triggers. A bounded queue should stay at or below " + MAX_IN_FLIGHT_ACKS
                + " regardless of how many triggers ran (see issue #53)", retained <= MAX_IN_FLIGHT_ACKS);
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
