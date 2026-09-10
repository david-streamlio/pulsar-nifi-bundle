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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Regression test for issue #53. Acknowledgements in async mode are submitted to an
 * ExecutorCompletionService, which retains the Future of every completed task until it is taken. Nothing
 * took them, so the queue grew by one Future per acknowledgement for the lifetime of the processor.
 * <p>
 * The leak is a property of what happens <i>after</i> an acknowledgement completes: with the drain in
 * place a completed Future is taken by the next trigger; without it, it stays queued for good. So the test
 * waits for every submitted acknowledgement to complete, lets one more trigger drain, and asserts that
 * nothing is left. That is the leak and nothing else - no allowance for "acks still in flight", which
 * depends on how loaded the machine is and cannot be pinned (#233: a hard 3 failed in CI with 8, a quarter
 * of the triggers failed with 26 and 21, on commits that could not touch retention).
 * <p>
 * Runs for both subscription types the consumers acknowledge differently on: Shared acknowledges every
 * message, Exclusive cumulatively once per batch.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarAckLeakTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/events";
    private static final int TRIGGERS = 40;

    @Parameters(name = "{0}")
    public static Collection<Object[]> subscriptionTypes() {
        return Arrays.asList(new Object[][] {{"Shared"}, {"Exclusive"}});
    }

    private final String subscriptionType;

    public ConsumePulsarAckLeakTest(final String subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    /** Exposes the ack pool and completion service, which are protected on AbstractPulsarConsumerProcessor. */
    public static class AckProbeConsumePulsar extends ConsumePulsar {

        /**
         * How many submitted acknowledgements have not run yet. This is the number the old fixed allowance
         * tried to bound, and it is a property of the machine, not of the processor: logged for the record,
         * never asserted on.
         */
        long pendingAcks() {
            final ExecutorService pool = getAckPool();
            if (!(pool instanceof ThreadPoolExecutor)) {
                // Loud on purpose. Returning 0 here would make awaitSubmittedAcksToComplete() return without
                // waiting and pass its own check trivially, and the gate would silently go back to measuring
                // acks in flight on a loaded machine - the flake this test exists to be rid of (#233).
                throw new AssertionError("the ack pool is a " + (pool == null ? "null" : pool.getClass().getName())
                        + "; pendingAcks() can only measure a ThreadPoolExecutor, so this test cannot take the "
                        + "machine out of the measurement (see #233)");
            }
            final ThreadPoolExecutor executor = (ThreadPoolExecutor) pool;
            return executor.getTaskCount() - executor.getCompletedTaskCount();
        }

        /**
         * Waits until every acknowledgement submitted so far has run. The pool is a ThreadPoolExecutor with an
         * unbounded queue, so "submitted" and "completed" can be arbitrarily far apart on a loaded machine;
         * this is the wait that takes the machine out of the measurement.
         */
        void awaitSubmittedAcksToComplete() throws InterruptedException {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (pendingAcks() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals("acknowledgements still running after 30 s", 0L, pendingAcks());
        }

        /** Drains and counts the acknowledgement Futures the processor left behind. */
        int countRetainedAcks() throws InterruptedException {
            if (getAckService() == null) {
                return 0;
            }

            int retained = 0;
            Future<Object> ack = getAckService().poll(500, TimeUnit.MILLISECONDS);

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
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, subscriptionType);
    }

    /**
     * The gate. After the triggers have run and every acknowledgement they submitted has completed, one
     * trigger on the now-empty topic drains the completion service, and nothing may be left in it. With the
     * leak, everything is left: one Future per acknowledgement, {@link #TRIGGERS} of them.
     */
    @Test
    public void completedAcknowledgementsAreDrainedByTheNextTrigger() throws Exception {
        mockClientService.setMockMessageQueue(messages(TRIGGERS));

        // do not stop the processor: @OnUnscheduled tears the pools down, which would hide the leak
        runner.run(TRIGGERS, false);
        // diagnostic only - the in-flight count at this instant is what a loaded runner inflates (#233)
        System.out.println(subscriptionType + ": " + processor.pendingAcks() + " acknowledgement(s) still pending when the last of "
                + TRIGGERS + " triggers returned");
        processor.awaitSubmittedAcksToComplete();

        // the topic is empty now; this trigger receives nothing and only drains
        runner.run(1, false, false);

        final int retained = processor.countRetainedAcks();
        assertEquals("Acknowledgement Futures are being retained after they completed: " + retained + " left after "
                + TRIGGERS + " triggers and a draining one. The leak kept one per acknowledgement (see issue #53)",
                0, retained);
    }

    /**
     * The aggravating case: an idle topic. The cumulative-ack task used to be submitted outside the
     * "did we receive anything?" guard, so every trigger queued a Future holding an
     * IndexOutOfBoundsException from messages.get(-1) - an idle processor leaked fastest of all.
     * <p>
     * That is a defect of the cumulative path, which only Exclusive takes: a Shared subscription acknowledges
     * per message and submits nothing on an idle topic, so its run of this test could only ever assert
     * {@code 0 == 0}. Pinned to Exclusive; the gate test above is where both types earn their place.
     */
    @Test
    public void idleTopicDoesNotQueueFailedAcks() throws Exception {
        assumeTrue("the idle-topic defect lives on the cumulative-ack path, which only Exclusive takes",
                "Exclusive".equals(subscriptionType));
        mockClientService.setMockMessageQueue(new ArrayList<>());

        runner.run(TRIGGERS, false);
        processor.awaitSubmittedAcksToComplete();

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
