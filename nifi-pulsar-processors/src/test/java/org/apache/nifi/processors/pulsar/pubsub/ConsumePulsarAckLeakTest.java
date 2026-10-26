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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
 */
public class ConsumePulsarAckLeakTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/events";
    private static final int TRIGGERS = 40;

    /** Exposes the ack pool and completion service, which are protected on AbstractPulsarConsumerProcessor. */
    public static class AckProbeConsumePulsar extends ConsumePulsar {
        /** Blocks until every acknowledgement submitted so far has finished running. */
        void awaitAcknowledgements() throws InterruptedException {
            final ThreadPoolExecutor pool = (ThreadPoolExecutor) getAckPool();
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
        int countRetainedAcks() {
            if (getAckService() == null) {
                return 0;
            }

            int retained = 0;
            while (getAckService().poll() != null) {
                retained++;
            }

            return retained;
        }
    }

    private AckProbeConsumePulsar processor;

    @BeforeEach
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

        final int retained = retainedAfterRunning(TRIGGERS);
        assertEquals(0, retained, "Acknowledgement Futures are being retained: " + retained + " left after "
                + TRIGGERS + " triggers. The drain should have taken every completed acknowledgement; the leak "
                + "left one per acknowledgement (see issue #53)");
    }

    /** Exclusive subscriptions acknowledge cumulatively, once per batch. */
    @Test
    public void exclusiveSubscriptionAcksAreNotRetained() throws Exception {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        mockClientService.setMockMessageQueue(messages(TRIGGERS));

        final int retained = retainedAfterRunning(TRIGGERS);
        assertEquals(0, retained, "Acknowledgement Futures are being retained: " + retained + " left after "
                + TRIGGERS + " triggers. The drain should have taken every completed acknowledgement; the leak "
                + "left one per acknowledgement (see issue #53)");
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

        final int retained = retainedAfterRunning(TRIGGERS);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        assertEquals(0, retained, "An idle topic queued " + retained + " acknowledgement Futures over "
                + TRIGGERS + " triggers; it should queue none (see issue #53)");
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
        processor.awaitAcknowledgements();
        runner.run(1, false, false);
        return processor.countRetainedAcks();
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
