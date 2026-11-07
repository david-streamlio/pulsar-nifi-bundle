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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression test for issue #53. Acknowledgements in async mode are submitted to an
 * ExecutorCompletionService, which retains the Future of every completed task until it is taken. Nothing
 * took them, so the queue grew by one Future per acknowledgement for the lifetime of the processor.
 * <p>
 * The test measures the leak behaviourally: it runs the processor, waits until every acknowledgement has
 * finished, runs one more (idle) trigger so the drain has seen all of them, and then counts the Futures still
 * sitting in the completion service. With the drain in place that count is exactly zero; with the leak it is
 * one per acknowledgement.
 * <p>
 * Earlier versions counted straight after the last trigger and allowed for acknowledgements still in flight.
 * How many were in flight depended on how quickly the ack pool's threads got CPU, so the test measured
 * scheduling rather than the leak and failed on loaded CI runners however generous the allowance: a hard 3
 * failed with 8, and a quarter of the triggers failed with 16. Waiting for the pool to go idle first takes
 * scheduling out of the result.
 * <p>
 * Runs against both consumers, because each has its own drain call at the end of its own, differently shaped,
 * async loop (#239), and against both Shared and Exclusive subscriptions.
 */
public class ConsumePulsarAckLeakTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/events";

    /**
     * Triggers per case. ConsumePulsar's async loop returns as soon as its receive completes, so forty triggers
     * take a moment. ConsumePulsarRecord's loops on its completion service until a poll times out, so every
     * trigger costs one Max Wait Time - set to its one-second minimum below, since the property reaches the
     * client in whole seconds. The leak is still one Future per acknowledgement, and ten against zero is as
     * unmistakable as forty.
     */
    private static final int TRIGGERS = 40;
    private static final int RECORD_TRIGGERS = 10;

    /**
     * What the test needs from a consumer: the ack pool and its completion service, which are protected on
     * AbstractPulsarConsumerProcessor. A probe subclass of each consumer exposes the two accessors; the
     * measurement is built on them here, once, so both consumers go through exactly the same one.
     */
    interface AckProbe {
        ExecutorService ackPool();

        ExecutorCompletionService<Object> ackService();

        /** Blocks until every acknowledgement submitted so far has finished running. */
        default void awaitAcknowledgements() throws InterruptedException {
            final ThreadPoolExecutor pool = (ThreadPoolExecutor) ackPool();
            if (pool == null) {
                return;
            }

            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (pool.getCompletedTaskCount() < pool.getTaskCount()) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("Acknowledgements still running after 30 seconds: "
                            + pool.getCompletedTaskCount() + " of " + pool.getTaskCount() + " finished");
                }
                Thread.sleep(10);
            }
        }

        /** Takes and counts the acknowledgement Futures still sitting in the completion service. */
        default int countRetainedAcks() {
            if (ackService() == null) {
                return 0;
            }

            int retained = 0;
            while (ackService().poll() != null) {
                retained++;
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

    private static final Supplier<AbstractPulsarConsumerProcessor<?>> CONSUME_PULSAR = AckProbeConsumePulsar::new;
    private static final Supplier<AbstractPulsarConsumerProcessor<?>> CONSUME_PULSAR_RECORD = AckProbeConsumePulsarRecord::new;

    public static Stream<Arguments> processorsAndSubscriptionTypes() {
        return Stream.of(
                Arguments.of("ConsumePulsar", "Shared", CONSUME_PULSAR),
                Arguments.of("ConsumePulsar", "Exclusive", CONSUME_PULSAR),
                Arguments.of("ConsumePulsarRecord", "Shared", CONSUME_PULSAR_RECORD),
                Arguments.of("ConsumePulsarRecord", "Exclusive", CONSUME_PULSAR_RECORD));
    }

    public static Stream<Arguments> processors() {
        return Stream.of(
                Arguments.of("ConsumePulsar", CONSUME_PULSAR),
                Arguments.of("ConsumePulsarRecord", CONSUME_PULSAR_RECORD));
    }

    private AbstractPulsarConsumerProcessor<?> processor;
    private AckProbe probe;

    /** Called by each test with its parameters, as a parameterised test has no constructor to set them in. */
    private void init(final Supplier<AbstractPulsarConsumerProcessor<?>> processorFactory, final String subscriptionType)
            throws InitializationException {
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
            // one string field per message; the payloads below are single-column lines
            final MockRecordParser reader = new MockRecordParser();
            reader.addSchemaField("payload", RecordFieldType.STRING);
            runner.addControllerService("record-reader", reader);
            runner.enableControllerService(reader);
            final MockRecordWriter writer = new MockRecordWriter("payload");
            runner.addControllerService("record-writer", writer);
            runner.enableControllerService(writer);
            runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
            runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
            runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "1 sec");
        }
    }

    private int triggers() {
        return processor instanceof ConsumePulsarRecord ? RECORD_TRIGGERS : TRIGGERS;
    }

    /**
     * The gate. After the triggers have run and every acknowledgement they submitted has finished, one trigger
     * on the now-empty topic drains the completion service, and nothing may be left in it. With the leak,
     * everything is left: one Future per acknowledgement.
     */
    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("processorsAndSubscriptionTypes")
    public void acknowledgementsAreNotRetained(final String processorName, final String subscriptionType,
            final Supplier<AbstractPulsarConsumerProcessor<?>> processorFactory) throws Exception {
        init(processorFactory, subscriptionType);
        final int triggers = triggers();
        mockClientService.setMockMessageQueue(messages(triggers));

        final int retained = retainedAfterRunning(triggers);
        assertEquals(0, retained, processorName + " " + subscriptionType + ": acknowledgement Futures are being "
                + "retained: " + retained + " left after " + triggers + " triggers. The drain should have taken "
                + "every completed acknowledgement; the leak left one per acknowledgement (see issue #53)");
    }

    /**
     * The aggravating case: an idle topic. The cumulative-ack task used to be submitted outside the
     * "did we receive anything?" guard, so every trigger queued a Future holding an
     * IndexOutOfBoundsException from messages.get(-1) - an idle processor leaked fastest of all. Exclusive was
     * the subscription type that took that path. Both consumers run it, since each guards the submission in
     * its own loop (#239).
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("processors")
    public void idleTopicDoesNotQueueFailedAcks(final String processorName,
            final Supplier<AbstractPulsarConsumerProcessor<?>> processorFactory) throws Exception {
        init(processorFactory, "Exclusive");
        mockClientService.setMockMessageQueue(new ArrayList<>());

        final int retained = retainedAfterRunning(triggers());

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        assertEquals(0, retained, processorName + ": an idle topic queued " + retained + " acknowledgement "
                + "Futures over " + triggers() + " triggers; it should queue none (see issue #53)");
    }

    /**
     * Runs the processor for the given number of triggers, waits for every acknowledgement it submitted to
     * finish, then runs one more trigger so the drain has seen all of them, and counts what is left.
     * <p>
     * The processor is never stopped: {@code @OnUnscheduled} tears the pools down, which would hide the
     * leak. The extra trigger finds the topic empty, so it submits nothing and only drains; it runs without
     * re-initialising, because {@code @OnScheduled} would rebuild the ack pool and lose the Futures.
     */
    private int retainedAfterRunning(final int triggers) throws InterruptedException {
        runner.run(triggers, false);
        probe.awaitAcknowledgements();
        runner.run(1, false, false);
        return probe.countRetainedAcks();
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
