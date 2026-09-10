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
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
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
 * Runs for both consumers - each has its own drain call at the end of its own async loop (#239) - and for
 * both subscription types the consumers acknowledge differently on: Shared acknowledges every message,
 * Exclusive cumulatively once per batch.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarAckLeakTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/events";

    /**
     * Triggers per case. ConsumePulsar's async loop returns as soon as its one receive future completes, so
     * forty triggers take a moment. ConsumePulsarRecord's loops on the completion service until a poll times
     * out, so every trigger costs one Max Wait Time (set to its one-second minimum below) - the leak it measures
     * is the same one Future per acknowledgement, and ten of them against zero is as unmistakable as forty.
     */
    private static final int TRIGGERS = 40;
    private static final int RECORD_TRIGGERS = 10;

    @Parameters(name = "{0} {1}")
    public static Collection<Object[]> processorsAndSubscriptionTypes() {
        final List<Object[]> cases = new ArrayList<>();
        for (final String subscriptionType : new String[] {"Shared", "Exclusive"}) {
            cases.add(new Object[] {"ConsumePulsar", subscriptionType, (Supplier<AbstractPulsarConsumerProcessor<?>>) AckProbeConsumePulsar::new});
            cases.add(new Object[] {"ConsumePulsarRecord", subscriptionType, (Supplier<AbstractPulsarConsumerProcessor<?>>) AckProbeConsumePulsarRecord::new});
        }
        return cases;
    }

    private final String subscriptionType;
    private final Supplier<AbstractPulsarConsumerProcessor<?>> processorFactory;

    public ConsumePulsarAckLeakTest(final String processorName, final String subscriptionType,
                                    final Supplier<AbstractPulsarConsumerProcessor<?>> processorFactory) {
        this.subscriptionType = subscriptionType;
        this.processorFactory = processorFactory;
    }

    /**
     * What the test needs from a consumer processor: the ack pool and its completion service, which are protected
     * on AbstractPulsarConsumerProcessor. A probe subclass of each consumer exposes the two accessors; everything
     * the test measures is built on them here, once, so both processors go through the same measurement.
     */
    interface AckProbe {
        ExecutorService ackPool();

        ExecutorCompletionService<Object> ackService();

        /**
         * How many submitted acknowledgements have not run yet. This is the number the old fixed allowance
         * tried to bound, and it is a property of the machine, not of the processor: logged for the record,
         * never asserted on.
         */
        default long pendingAcks() {
            final ExecutorService pool = ackPool();
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
        default void awaitSubmittedAcksToComplete() throws InterruptedException {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (pendingAcks() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals("acknowledgements still running after 30 s", 0L, pendingAcks());
        }

        /** Drains and counts the acknowledgement Futures the processor left behind. */
        default int countRetainedAcks() throws InterruptedException {
            if (ackService() == null) {
                return 0;
            }

            int retained = 0;
            Future<Object> ack = ackService().poll(500, TimeUnit.MILLISECONDS);

            while (ack != null) {
                retained++;
                ack = ackService().poll(100, TimeUnit.MILLISECONDS);
            }

            return retained;
        }
    }

    public static class AckProbeConsumePulsar extends ConsumePulsar implements AckProbe {
        @Override
        public ExecutorService ackPool() {
            return getAckPool();
        }

        @Override
        public ExecutorCompletionService<Object> ackService() {
            return getAckService();
        }
    }

    public static class AckProbeConsumePulsarRecord extends ConsumePulsarRecord implements AckProbe {
        @Override
        public ExecutorService ackPool() {
            return getAckPool();
        }

        @Override
        public ExecutorCompletionService<Object> ackService() {
            return getAckService();
        }
    }

    private AbstractPulsarConsumerProcessor<?> processor;
    private AckProbe probe;

    @Before
    public void init() throws InitializationException {
        processor = processorFactory.get();
        probe = (AckProbe) processor;
        runner = TestRunners.newTestRunner(processor);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "true");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, subscriptionType);

        if (processor instanceof ConsumePulsarRecord) {
            // one string field per message; the payloads below are single-column CSV lines
            final MockRecordParser reader = new MockRecordParser();
            reader.addSchemaField("payload", RecordFieldType.STRING);
            runner.addControllerService("record-reader", reader);
            runner.enableControllerService(reader);
            final MockRecordWriter writer = new MockRecordWriter("payload");
            runner.addControllerService("record-writer", writer);
            runner.enableControllerService(writer);
            runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
            runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
            // the record processor's async loop polls until this elapses, once per trigger; the property is
            // handed to the client in whole seconds, so one second is the shortest wait it can run with
            runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "1 sec");
        }
    }

    private int triggers() {
        return processor instanceof ConsumePulsarRecord ? RECORD_TRIGGERS : TRIGGERS;
    }

    /**
     * The gate. After the triggers have run and every acknowledgement they submitted has completed, one
     * trigger on the now-empty topic drains the completion service, and nothing may be left in it. With the
     * leak, everything is left: one Future per acknowledgement, one per trigger.
     */
    @Test
    public void completedAcknowledgementsAreDrainedByTheNextTrigger() throws Exception {
        final int triggers = triggers();
        mockClientService.setMockMessageQueue(messages(triggers));

        // do not stop the processor: @OnUnscheduled tears the pools down, which would hide the leak
        runner.run(triggers, false);
        // diagnostic only - the in-flight count at this instant is what a loaded runner inflates (#233)
        System.out.println(processor.getClass().getSimpleName() + " " + subscriptionType + ": " + probe.pendingAcks()
                + " acknowledgement(s) still pending when the last of " + triggers + " triggers returned");
        probe.awaitSubmittedAcksToComplete();

        // the topic is empty now; this trigger receives nothing and only drains
        runner.run(1, false, false);

        final int retained = probe.countRetainedAcks();
        assertEquals("Acknowledgement Futures are being retained after they completed: " + retained + " left after "
                + triggers + " triggers and a draining one. The leak kept one per acknowledgement (see issue #53)",
                0, retained);
    }

    /**
     * The aggravating case: an idle topic. The cumulative-ack task used to be submitted outside the
     * "did we receive anything?" guard, so every trigger queued a Future holding an
     * IndexOutOfBoundsException from messages.get(-1) - an idle processor leaked fastest of all.
     * <p>
     * That is a defect of the cumulative path, which only Exclusive takes: a Shared subscription acknowledges
     * per message and submits nothing on an idle topic, so its run of this test could only ever assert
     * {@code 0 == 0}. Pinned to Exclusive; the gate test above is where both types earn their place. Both
     * processors run it, since each guards the submission in its own loop (#239).
     */
    @Test
    public void idleTopicDoesNotQueueFailedAcks() throws Exception {
        assumeTrue("the idle-topic defect lives on the cumulative-ack path, which only Exclusive takes",
                "Exclusive".equals(subscriptionType));
        mockClientService.setMockMessageQueue(new ArrayList<>());

        runner.run(triggers(), false);
        probe.awaitSubmittedAcksToComplete();

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        final int retained = probe.countRetainedAcks();
        assertTrue("An idle topic queued " + retained + " acknowledgement Futures over " + triggers()
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
