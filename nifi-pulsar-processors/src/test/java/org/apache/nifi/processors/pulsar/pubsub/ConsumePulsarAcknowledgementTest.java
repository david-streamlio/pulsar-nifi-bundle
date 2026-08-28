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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockSessionFactory;
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
 * A message may be acknowledged to the broker only once the FlowFile that carries it has been committed.
 * <p>
 * An acknowledged message is gone from the subscription: the broker never redelivers it. ConsumePulsar
 * acknowledged Shared-subscription messages before writing them, and non-shared ones right after rolling
 * the session back on a write error, so any failure to write the content - a full content repository, a
 * permissions problem, a disk fault - lost the batch silently: it was neither in NiFi nor recoverable from
 * Pulsar. Every scenario runs in synchronous and asynchronous mode on a Shared and on an Exclusive
 * subscription, because each combination acknowledges through a different path.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarAcknowledgementTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/acknowledgement";

    @Parameters(name = "async={0}, subscription={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {false, "Exclusive"}, {false, "Shared"}, {true, "Exclusive"}, {true, "Shared"}});
    }

    private final boolean async;
    private final String subscriptionType;
    private final boolean shared;

    public ConsumePulsarAcknowledgementTest(final boolean async, final String subscriptionType) {
        this.async = async;
        this.subscriptionType = subscriptionType;
        this.shared = isSharedSubType(subscriptionType);
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
     * The healthy case, with the order that matters: by the time the broker is told a message is handled,
     * the FlowFile carrying it is already committed. Before the fix a Shared subscription acknowledged
     * every message before it was even written, and the committed count recorded here was 0.
     */
    @Test
    public void messagesAreAcknowledgedOnlyAfterTheirFlowFileIsCommitted() throws PulsarClientException {
        final List<String> statesAtAcknowledgement = recordSessionStateAtAcknowledgement(ConsumePulsar.REL_SUCCESS);
        mockClientService.setMockMessageQueue(messages(3, "payload"));

        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, 1);
        assertEquals("one acknowledgement per message on a Shared subscription, one cumulative acknowledgement otherwise",
                shared ? 3 : 1, statesAtAcknowledgement.size());
        for (final String state : statesAtAcknowledgement) {
            assertEquals("a message was acknowledged before the FlowFile carrying it was committed", ONE_COMMITTED_FLOWFILE, state);
        }
    }

    /**
     * The failure from the issue: the content repository rejects the write. Nothing may be acknowledged,
     * so that the broker redelivers the batch. Before the fix a Shared subscription had already acknowledged
     * every message, and an Exclusive one acknowledged cumulatively right after rolling the session back.
     */
    @Test
    public void nothingIsAcknowledgedWhenTheContentCannotBeWritten() throws PulsarClientException {
        // schedule the processor against the still-empty topic
        runner.run(1, false, true);
        mockClientService.setMockMessageQueue(messages(3, "payload"));
        final MockProcessSession session = sessionWhoseContentCannotBeWritten();

        ((ConsumePulsar) runner.getProcessor()).onTrigger(runner.getProcessContext(), session);
        // what the framework does once onTrigger returns: it has to find a clean, rolled-back session
        session.commitAsync();
        // unschedule the processor, which waits for the acknowledgement pool to finish whatever was
        // submitted. (Running another trigger instead would consume what the failed one left in the
        // receiver queue - in synchronous mode the write fails on the first message - and acknowledge it.)
        ((AbstractPulsarConsumerProcessor<?>) runner.getProcessor()).shutDown(runner.getProcessContext());

        session.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        verifyNothingAcknowledged();
    }

    /** Messages with an empty payload are discarded on purpose, and they still have to be acknowledged. */
    @Test
    public void discardedEmptyMessagesAreStillAcknowledged() throws PulsarClientException {
        mockClientService.setMockMessageQueue(messages(3, null));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        verifyAcknowledged(3);
    }

    /**
     * Stubs every acknowledgement method of the consumer to record the state of the session at the moment
     * the broker is told a message is handled: how many FlowFiles the relationship holds, and whether the
     * session has been committed. The expected value is {@link #ONE_COMMITTED_FLOWFILE}.
     */
    private List<String> recordSessionStateAtAcknowledgement(final Relationship relationship) throws PulsarClientException {
        final List<String> states = new CopyOnWriteArrayList<>();
        final Consumer<GenericRecord> consumer = mockClientService.getMockConsumer();

        doAnswer(invocation -> {
            states.add(sessionState(relationship));
            return null;
        }).when(consumer).acknowledge(any(Message.class));
        doAnswer(invocation -> {
            states.add(sessionState(relationship));
            return null;
        }).when(consumer).acknowledgeCumulative(any(Message.class));
        doAnswer(invocation -> {
            states.add(sessionState(relationship));
            return CompletableFuture.completedFuture(null);
        }).when(consumer).acknowledgeAsync(any(Message.class));
        doAnswer(invocation -> {
            states.add(sessionState(relationship));
            return CompletableFuture.completedFuture(null);
        }).when(consumer).acknowledgeCumulativeAsync(any(Message.class));

        return states;
    }

    private static final String ONE_COMMITTED_FLOWFILE = "1 transferred, committed";

    private String sessionState(final Relationship relationship) {
        final int transferred = runner.getFlowFilesForRelationship(relationship).size();
        final boolean committed = ((MockSessionFactory) runner.getProcessSessionFactory()).getCreatedSessions()
                .stream().allMatch(ConsumePulsarAcknowledgementTest::isCommitted);

        return transferred + " transferred, " + (committed ? "committed" : "not committed");
    }

    private static boolean isCommitted(final MockProcessSession session) {
        try {
            session.assertCommitted();
            return true;
        } catch (final AssertionError notCommitted) {
            return false;
        }
    }

    private void verifyAcknowledged(final int messages) throws PulsarClientException {
        final Consumer<GenericRecord> consumer = mockClientService.getMockConsumer();

        if (shared) {
            verify(consumer, times(async ? 0 : messages)).acknowledge(any(Message.class));
            verify(consumer, times(async ? messages : 0)).acknowledgeAsync(any(Message.class));
        } else {
            verify(consumer, times(async ? 0 : 1)).acknowledgeCumulative(any(Message.class));
            verify(consumer, times(async ? 1 : 0)).acknowledgeCumulativeAsync(any(Message.class));
        }
    }

    private void verifyNothingAcknowledged() throws PulsarClientException {
        final Consumer<GenericRecord> consumer = mockClientService.getMockConsumer();

        verify(consumer, never()).acknowledge(any(Message.class));
        verify(consumer, never()).acknowledgeAsync(any(Message.class));
        verify(consumer, never()).acknowledgeCumulative(any(Message.class));
        verify(consumer, never()).acknowledgeCumulativeAsync(any(Message.class));
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
