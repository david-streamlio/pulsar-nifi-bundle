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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * A message left outstanding by one trigger survives another trigger's acknowledgement.
 * <p>
 * This is the loss in #223, reached without threading. Cumulative acknowledgement means "this message and
 * everything before it on the subscription", so acknowledging a later message acknowledged an earlier one
 * that a previous trigger had rolled back and negatively acknowledged. Already acknowledged, it was never
 * redelivered - neither in NiFi nor recoverable from Pulsar.
 * <p>
 * Only a broker can show this. The unit test asserts that cumulative acknowledgement is not called; whether
 * the earlier message actually comes back is the broker's decision, and it is the thing that matters.
 */
public class ConsumePulsarConcurrentAckIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        // Exclusive is the subscription type that used cumulative acknowledgement.
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        // One message per trigger, so the two messages are handled by separate triggers with separate
        // sessions - which is what lets one be outstanding while the other is acknowledged.
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        // Long, deliberately: the rolled-back message must stay outstanding while the next one is
        // committed. With a short delay it is redelivered before that happens and the interleaving the
        // defect needs never occurs - which is how the first version of this test passed against the bug.
        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "60 sec");
        runner.setProperty(AbstractPulsarConsumerProcessor.NEGATIVE_ACK_REDELIVERY_DELAY, "60 sec");
    }

    @Test
    public void aMessageRolledBackByOneTriggerIsNotAcknowledgedByTheNext() throws Exception {
        final String topic = "persistent://public/default/concurrent-ack-" + System.nanoTime();
        final String subscription = "concurrent-ack-sub";
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, subscription);

        // Schedule against the still-empty topic: an initializing run also triggers, and would otherwise
        // consume and commit "first" before the failing trigger ever saw it, shifting the whole sequence
        // by one message. That is what the first version of this test did.
        runner.run(1, false, true);

        publish(topic, "first", "second");

        // Trigger one takes "first" and cannot write it: the session rolls back and the message is
        // negatively acknowledged. With a 60s redelivery delay it stays outstanding for the rest of the
        // test - owed to the subscription, and not yet acknowledged by anyone.
        ((ConsumePulsar) runner.getProcessor()).onTrigger(runner.getProcessContext(), failingSession());

        // Trigger two takes "second" and commits it, which acknowledges it.
        await("the second message to be consumed and committed", () -> {
            runner.run(1, false, false);
            return consumed().contains("second");
        });

        // The assertion. "first" is still owed, so the subscription must still have a backlog. Under
        // cumulative acknowledgement, acknowledging "second" acknowledged everything before it on the
        // subscription - including "first" - and the backlog went to zero with "first" never delivered
        // anywhere. Checked through the broker rather than by waiting for a redelivery, so the test does
        // not have to distinguish "lost" from "not yet".
        final long backlog = backlogOf(topic, subscription);

        assertTrue("the message rolled back by the first trigger was acknowledged by the second trigger's "
                + "commit: the subscription has no backlog, so nothing will ever redeliver it",
                backlog >= 1);
        assertTrue("the second message was not consumed: " + consumed(), consumed().contains("second"));
    }

    /**
     * The number of messages the subscription still owes, read from the broker.
     *
     * <p>Parsed with plain string search rather than a JSON library: the topic stats are a large document
     * and the only thing needed is the msgBacklog that follows this subscription's name.
     */
    private static long backlogOf(final String topic, final String subscription) throws Exception {
        final String stats = exec("bin/pulsar-admin", "topics", "stats", topic);
        final int subscriptionAt = stats.indexOf("\"" + subscription + "\"");

        if (subscriptionAt < 0) {
            throw new AssertionError("subscription " + subscription + " not found in topic stats:\n" + stats);
        }

        final String key = "\"msgBacklog\"";
        final int backlogAt = stats.indexOf(key, subscriptionAt);

        if (backlogAt < 0) {
            throw new AssertionError("msgBacklog not found after the subscription in topic stats:\n" + stats);
        }

        final int colon = stats.indexOf(':', backlogAt + key.length());
        final StringBuilder digits = new StringBuilder();

        for (int i = colon + 1; i < stats.length(); i++) {
            final char c = stats.charAt(i);
            if (Character.isDigit(c)) {
                digits.append(c);
            } else if (digits.length() > 0) {
                break;
            }
        }

        return Long.parseLong(digits.toString());
    }

    /** Runs a command inside the broker container and returns its combined output. */
    private static String exec(final String... command) throws Exception {
        final org.testcontainers.containers.Container.ExecResult result = PULSAR.execInContainer(command);
        return result.getStdout() + result.getStderr();
    }

    /** Everything routed to success so far, concatenated. */
    private String consumed() {
        final StringBuilder sb = new StringBuilder();

        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS)) {
            sb.append(new String(flowFile.toByteArray())).append("\n");
        }

        return sb.toString();
    }

    /** A session whose FlowFile content cannot be written - a full or read-only content repository. */
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
