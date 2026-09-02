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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Before;
import org.junit.Test;

/**
 * Negative acknowledgement and the dead letter policy, against a real broker.
 * <p>
 * Neither can be proven with a mocked client: both are broker-side behaviour. A mock can show that
 * {@code negativeAcknowledge} was called - {@code ConsumePulsarNegativeAcknowledgementTest} does - but only
 * a broker decides whether that produces a redelivery, and only a broker moves a message to the dead letter
 * topic once the redelivery count is exceeded.
 */
public class ConsumePulsarDeadLetterIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        // The dead letter policy is built only for Shared and Key_Shared subscriptions.
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        // Redeliver as soon as the broker will, so the test does not wait on the one-minute default.
        runner.setProperty(AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY, "1 sec");
        // The floor the validator allows. The point of the redelivery test is that it completes well
        // inside this, which is the only thing that could have redelivered the message before.
        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "10 sec");
    }

    /**
     * A message the processor could not write is redelivered promptly, rather than after the Acknowledgment
     * Timeout. The assertion is the timing: the redelivery has to arrive inside the timeout that would
     * otherwise have been the only thing to produce it.
     */
    @Test
    public void aMessageThatCouldNotBeWrittenIsRedeliveredWithoutWaitingOutTheAckTimeout() throws Exception {
        final String topic = topic("nack-redelivery");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nack-sub");

        publish(topic, "payload");

        // First pass: the content repository rejects the write, so the batch is rolled back and nacked.
        runner.run(1, false, true);
        final long nackedAt = System.nanoTime();
        ((ConsumePulsar) runner.getProcessor()).onTrigger(runner.getProcessContext(), failingSession());

        // Second pass with a healthy session: the broker should hand the message back.
        await("the negatively acknowledged message to be redelivered", () -> {
            runner.run(1, false, false);
            return !runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).isEmpty();
        });

        final long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - nackedAt);
        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).get(0).assertContentEquals("payload");
        // 10s is the Acknowledgment Timeout, the only redelivery mechanism before the negative
        // acknowledgement existed. Landing inside it is what proves the nack did the work.
        org.junit.Assert.assertTrue(
                "redelivery took " + elapsed + "s, which is not distinguishable from the Acknowledgment Timeout",
                elapsed < 8);
    }

    /**
     * Once a message has been redelivered more times than Max Redelivery Count, the broker moves it to the
     * dead letter topic instead of delivering it again - so a message the flow can never accept stops
     * blocking the subscription, and is still available to look at.
     */
    @Test
    public void aRepeatedlyUndeliverableMessageEndsUpOnTheDeadLetterTopic() throws Exception {
        final String topic = topic("dead-letter");
        final String deadLetterTopic = topic + "-DLQ";
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "dlq-sub");
        runner.setProperty(AbstractPulsarConsumerProcessor.MAX_REDELIVER_COUNT, "2");
        runner.setProperty(AbstractPulsarConsumerProcessor.DEAD_LETTER_TOPIC, deadLetterTopic);

        // Subscribe to the dead letter topic before anything is published to it, so the message cannot be
        // missed by a subscription that starts at the latest position.
        try (Consumer<byte[]> deadLetters = getClient().newConsumer(Schema.BYTES)
                .topic(deadLetterTopic)
                .subscriptionName("dlq-reader")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {

            publish(topic, "poison");

            runner.run(1, false, true);

            // Each pass receives the message, fails to write it, rolls back and negatively acknowledges it.
            // The passes have to be spaced: redelivery is what increments the count the dead letter policy
            // measures, and it does not happen until Negative Acknowledgment Redelivery Delay has elapsed.
            // Triggering in a tight loop just finds an empty receiver queue every time after the first.
            Message<byte[]> deadLettered = null;
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

            while (deadLettered == null && System.nanoTime() < deadline) {
                ((ConsumePulsar) runner.getProcessor()).onTrigger(runner.getProcessContext(), failingSession());
                // Long enough for the negative acknowledgement tracker to fire and the broker to redeliver.
                deadLettered = deadLetters.receive(2, TimeUnit.SECONDS);
            }

            assertNotNull("the poison message never reached the dead letter topic", deadLettered);
            assertEquals("poison", new String(deadLettered.getValue(), UTF_8));
        }
    }

    private static String topic(final String name) {
        return "persistent://public/default/" + name + "-" + System.nanoTime();
    }

    /**
     * A session whose FlowFile content cannot be written - what a full or read-only content repository
     * looks like to the processor.
     */
    private MockProcessSession failingSession() {
        final Processor processor = runner.getProcessor();

        return new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor)) {
            @Override
            public OutputStream write(final FlowFile flowFile) {
                return new OutputStream() {
                    @Override
                    public void write(final int b) throws IOException {
                        throw new IOException("Intentional Integration Test Exception: the content repository cannot be written");
                    }
                };
            }
        };
    }
}
