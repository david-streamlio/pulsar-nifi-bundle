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
package org.apache.nifi.processors.pulsar.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

/**
 * On an Exclusive or Failover subscription the processor used to acknowledge a batch cumulatively, up to its
 * last message. A cumulative acknowledgement covers everything before that message on the subscription - not
 * only the batch - and two kinds of message can be "before" it without belonging to the batch (#223):
 * <ul>
 *   <li>one this task received earlier, failed to write and negatively acknowledged, which is waiting for
 *       redelivery - a single task is enough to lose it;</li>
 *   <li>one a concurrent task is still holding and may yet fail to write.</li>
 * </ul>
 * Either way the message was acknowledged by a batch it was not part of, and when its own redelivery came due the
 * broker had nothing to redeliver. Both sequences are run here on both subscription types; Shared is the control,
 * since it has always acknowledged per message.
 */
public class ConsumePulsarNonSharedAcknowledgementIT extends AbstractPulsarIT {

    private static final String[] NON_SHARED = {"Exclusive", "Failover"};

    /** Well inside the write-failure loops below, and what a message nacked at t=0 has to survive. */
    private static final long REDELIVERY_DELAY_SECONDS = 5;

    private static final String WRITE_FAILED = "Unable to write the received messages";

    private TestRunner runner(final String subscriptionType, final String topic) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, subscriptionType);
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "ack-sub");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        // one message per batch, so the batches of the two sequences are exactly the messages named below
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        runner.setProperty(AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY, REDELIVERY_DELAY_SECONDS + " sec");
        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "10 sec");
        return runner;
    }

    private static String topic(final String name, final String subscriptionType) {
        return "persistent://public/default/non-shared-ack-" + name + "-" + subscriptionType.toLowerCase() + "-" + System.nanoTime();
    }

    /**
     * One task. {@code m1} is received and its write fails, so it is rolled back and negatively acknowledged.
     * {@code m2} arrives before the redelivery delay elapses and is written, committed and acknowledged. That
     * acknowledgement must not take {@code m1} with it: {@code m1} has to come back once its delay is up.
     */
    @Test
    public void aNegativelyAcknowledgedMessageSurvivesTheAcknowledgementOfALaterOne() throws Exception {
        for (final String subscriptionType : NON_SHARED) {
            final String topic = topic("single-task", subscriptionType);
            final TestRunner runner = runner(subscriptionType, topic);

            runner.run(1, false, true);
            publish(topic, "m1");
            final long nackedAt = failWriteOfNextMessage(runner);

            publish(topic, "m2");
            await(subscriptionType + ": m2 to be consumed while m1 waits for redelivery", () -> {
                runner.run(1, false, false);
                return !runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).isEmpty();
            });
            final long m2ConsumedAfterSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - nackedAt);
            assertEquals(subscriptionType, List.of("m2"), received(runner));
            assertTrue(subscriptionType + ": m2 was consumed " + m2ConsumedAfterSeconds + " s after the nack, not inside "
                    + "the redelivery delay, so the sequence under test did not happen", m2ConsumedAfterSeconds < REDELIVERY_DELAY_SECONDS);
            runner.clearTransferState();

            // m1's redelivery is due; a cumulative acknowledgement of m2 would have taken it along
            assertEquals(subscriptionType + ": m1 was negatively acknowledged and must be redelivered",
                    List.of("m1"), consumeFor(runner, REDELIVERY_DELAY_SECONDS + 15));
        }
    }

    /**
     * Two tasks on one consumer, the interleaving from the issue. Task A receives {@code m1} and is still writing
     * it when task B receives {@code m2}, writes it, commits and acknowledges. A's write then fails and A negatively
     * acknowledges {@code m1}. B's acknowledgement must not have covered {@code m1}: it has to come back.
     */
    @Test
    public void aMessageHeldByAConcurrentTaskSurvivesTheOtherTasksAcknowledgement() throws Exception {
        for (final String subscriptionType : NON_SHARED) {
            final String topic = topic("two-tasks", subscriptionType);
            final TestRunner runner = runner(subscriptionType, topic);

            runner.run(1, false, true);
            publish(topic, "m1", "m2");

            // task A: receives m1 and blocks inside the write until told to fail
            final CountDownLatch aIsWriting = new CountDownLatch(1);
            final CountDownLatch bHasAcknowledged = new CountDownLatch(1);
            final Thread taskA = new Thread(() -> {
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (aIsWriting.getCount() > 0 && System.nanoTime() < deadline) {
                    ((ConsumePulsar) runner.getProcessor()).onTrigger(runner.getProcessContext(),
                            failingSession(runner, aIsWriting, bHasAcknowledged));
                }
            }, "task-A");
            taskA.start();
            assertTrue(subscriptionType + ": task A never received m1", aIsWriting.await(30, TimeUnit.SECONDS));

            // task B: receives m2, writes it, commits, acknowledges - while A still holds m1
            await(subscriptionType + ": task B to consume m2 while task A holds m1", () -> {
                runner.run(1, false, false);
                return !runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).isEmpty();
            });
            assertEquals(subscriptionType, List.of("m2"), received(runner));
            runner.clearTransferState();

            // now A's write fails: m1 is rolled back and negatively acknowledged
            bHasAcknowledged.countDown();
            taskA.join(TimeUnit.SECONDS.toMillis(30));
            assertTrue(subscriptionType + ": task A did not fail its write", writeFailuresLogged(runner) > 0);

            assertEquals(subscriptionType + ": m1, held by task A when task B acknowledged m2, must be redelivered",
                    List.of("m1"), consumeFor(runner, REDELIVERY_DELAY_SECONDS + 15));
        }
    }

    /** Healthy passes for the given time, collecting what arrives; a message the broker still holds shows up here. */
    private static List<String> consumeFor(final TestRunner runner, final long seconds) throws Exception {
        final List<String> all = new ArrayList<>();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            runner.run(1, false, false);
            all.addAll(received(runner));
            runner.clearTransferState();
            Thread.sleep(200);
        }
        return all;
    }

    private static List<String> received(final TestRunner runner) {
        final List<String> payloads = new ArrayList<>();
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS)) {
            for (final String line : new String(flowFile.toByteArray(), UTF_8).split("\n")) {
                if (!line.isEmpty()) {
                    payloads.add(line);
                }
            }
        }
        return payloads;
    }

    /** Triggers with a session that cannot be written until a message has been received and refused. */
    private static long failWriteOfNextMessage(final TestRunner runner) throws InterruptedException {
        final long failuresBefore = writeFailuresLogged(runner);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            ((ConsumePulsar) runner.getProcessor()).onTrigger(runner.getProcessContext(), failingSession(runner, null, null));
            if (writeFailuresLogged(runner) > failuresBefore) {
                return System.nanoTime();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("the message never reached the pass whose write fails");
    }

    private static long writeFailuresLogged(final TestRunner runner) {
        return runner.getLogger().getErrorMessages().stream().filter(m -> m.getMsg().contains(WRITE_FAILED)).count();
    }

    /**
     * A session whose FlowFile content cannot be written. With latches, the write first announces that it has
     * started and then waits to be told to fail, so another task can be run in between.
     */
    private static MockProcessSession failingSession(final TestRunner runner, final CountDownLatch writing,
                                                     final CountDownLatch failWhen) {
        final Processor processor = runner.getProcessor();
        return new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor)) {
            @Override
            public OutputStream write(final FlowFile flowFile) {
                return new OutputStream() {
                    @Override
                    public void write(final int b) throws IOException {
                        if (writing != null) {
                            writing.countDown();
                            try {
                                if (!failWhen.await(60, TimeUnit.SECONDS)) {
                                    throw new IOException("the other task never acknowledged");
                                }
                            } catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        throw new IOException("Intentional Integration Test Exception: the content repository cannot be written");
                    }
                };
            }
        };
    }
}
