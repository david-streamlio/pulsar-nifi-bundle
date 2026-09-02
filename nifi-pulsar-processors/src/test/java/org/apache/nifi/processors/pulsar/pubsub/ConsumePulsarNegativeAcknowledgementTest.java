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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A message the processor could not hand to the flow is negatively acknowledged, not merely left unacknowledged.
 * <p>
 * Both leave the message on the subscription, so neither loses it - {@link ConsumePulsarAcknowledgementTest}
 * covers that guarantee. The difference is how long redelivery takes. An unacknowledged message is
 * indistinguishable to the broker from one a healthy consumer is still working on, so it waits out the
 * Acknowledgment Timeout: thirty seconds by default and never less than ten, by validation. The processor
 * never called {@code negativeAcknowledge} at all, so every write error stalled the batch for that long.
 * <p>
 * Runs in synchronous and asynchronous mode on a Shared and on an Exclusive subscription, matching the
 * acknowledgement suite: each combination reaches the rollback through a different path.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarNegativeAcknowledgementTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/negative-acknowledgement";

    @Parameters(name = "async={0}, subscription={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {false, "Exclusive"}, {false, "Shared"}, {true, "Exclusive"}, {true, "Shared"}});
    }

    private final boolean async;
    private final String subscriptionType;

    public ConsumePulsarNegativeAcknowledgementTest(final boolean async, final String subscriptionType) {
        this.async = async;
        this.subscriptionType = subscriptionType;
    }

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, subscriptionType);
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
    }

    /**
     * The regression: the content repository rejects the write, so the batch is rolled back. Every message
     * the rolled-back session carried is negatively acknowledged, which is what asks the broker to redeliver
     * now rather than when the Acknowledgment Timeout expires. Before this change nothing was negatively
     * acknowledged and the flow waited out that timeout.
     */
    @Test
    public void messagesAreNegativelyAcknowledgedWhenTheContentCannotBeWritten() throws PulsarClientException {
        // schedule the processor against the still-empty topic
        runner.run(1, false, true);
        mockClientService.setMockMessageQueue(messages(3, "payload"));
        final MockProcessSession session = sessionWhoseContentCannotBeWritten();

        ((ConsumePulsar) runner.getProcessor()).onTrigger(runner.getProcessContext(), session);
        // what the framework does once onTrigger returns: it has to find a clean, rolled-back session
        session.commitAsync();
        ((AbstractPulsarConsumerProcessor<?>) runner.getProcessor()).shutDown(runner.getProcessContext());

        session.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        // The count varies with the path - the synchronous write fails on the first message, the
        // asynchronous one has the whole batch in hand - so what matters is that redelivery was asked
        // for at all, which is exactly what did not happen before.
        verify(mockClientService.getMockConsumer(), atLeastOnce()).negativeAcknowledge(any(Message.class));
    }

    /**
     * A message that reached a FlowFile is acknowledged, never negatively acknowledged. Nacking a message
     * the flow already owns would hand the same content to the flow twice.
     */
    @Test
    public void nothingIsNegativelyAcknowledgedWhenTheContentIsWritten() throws PulsarClientException {
        mockClientService.setMockMessageQueue(messages(3, "payload"));

        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, 1);
        verifyNothingNegativelyAcknowledged();
    }

    /**
     * Messages with an empty payload are discarded on purpose and acknowledged, so they are not redelivered.
     * A discard is a decision, not a failure.
     */
    @Test
    public void discardedEmptyMessagesAreNotNegativelyAcknowledged() throws PulsarClientException {
        mockClientService.setMockMessageQueue(messages(3, null));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        verifyNothingNegativelyAcknowledged();
    }

    private void verifyNothingNegativelyAcknowledged() throws PulsarClientException {
        final Consumer<GenericRecord> consumer = mockClientService.getMockConsumer();

        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    /**
     * A session whose FlowFile content cannot be written - what a full or read-only content repository
     * looks like to the processor. Everything else behaves like the runner's own session.
     */
    private MockProcessSession sessionWhoseContentCannotBeWritten() {
        final Processor processor = runner.getProcessor();

        return new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor)) {
            @Override
            public OutputStream write(final FlowFile flowFile) {
                return new OutputStream() {
                    @Override
                    public void write(final int b) throws IOException {
                        throw new IOException("Intentional Unit Test Exception: the content repository cannot be written");
                    }
                };
            }
        };
    }

    /** {@code count} messages with distinct ids; a {@code null} payload produces empty messages. */
    private static List<Message<GenericRecord>> messages(final int count, final String payload) {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();

        for (int n = 1; n <= count; n++) {
            final byte[] data = payload == null ? new byte[0] : (payload + "-" + n).getBytes(UTF_8);
            msgs.add(new MockPulsarMessage<GenericRecord>(TOPIC, data, "1234:" + n + ":0", null, null));
        }

        return msgs;
    }
}
