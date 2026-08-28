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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockFailingRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
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
 * The record processor has the same contract as {@link ConsumePulsarAcknowledgementTest}: a message is
 * acknowledged only once the FlowFile carrying it - on success or on parse_failure - has been committed,
 * and never when the session is rolled back.
 * <p>
 * ConsumePulsarRecord acknowledged Shared-subscription messages before parsing them, acknowledged
 * cumulatively after a failed write, and on a Shared subscription never acknowledged a message that could
 * not be parsed at all, so the broker redelivered it - and the flow received it again on parse_failure -
 * forever.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarRecordAcknowledgementTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/record-acknowledgement";

    @Parameters(name = "async={0}, subscription={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {false, "Exclusive"}, {false, "Shared"}, {true, "Exclusive"}, {true, "Shared"}});
    }

    private final boolean async;
    private final String subscriptionType;
    private final boolean shared;

    public ConsumePulsarRecordAcknowledgementTest(final boolean async, final String subscriptionType) {
        this.async = async;
        this.subscriptionType = subscriptionType;
        this.shared = isSharedSubType(subscriptionType);
    }

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addPulsarClientService();

        final MockRecordWriter writerService = new MockRecordWriter("name, age");
        runner.addControllerService("record-writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, subscriptionType);
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        // in async mode the processor keeps polling its completion service for this long after the last batch
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, async ? "1 sec" : "0 sec");
    }

    /** The healthy case: records reach success, and the acknowledgement follows the commit. */
    @Test
    public void messagesAreAcknowledgedOnlyAfterTheirFlowFileIsCommitted() throws Exception {
        useReader(parser());
        final List<String> statesAtAcknowledgement = recordSessionStateAtAcknowledgement(ConsumePulsarRecord.REL_SUCCESS);
        mockClientService.setMockMessageQueue(messages(3));

        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        assertEquals("one acknowledgement per message on a Shared subscription, one cumulative acknowledgement otherwise",
                shared ? 3 : 1, statesAtAcknowledgement.size());
        for (final String state : statesAtAcknowledgement) {
            assertEquals("a message was acknowledged before the FlowFile carrying it was committed", ONE_COMMITTED_FLOWFILE, state);
        }
    }

    /**
     * Messages that cannot be parsed are routed to parse_failure and acknowledged with that FlowFile.
     * Before the fix a Shared subscription skipped the acknowledgement for them entirely, so the broker
     * redelivered them on every acknowledgement timeout.
     */
    @Test
    public void unparseableMessagesAreAcknowledgedOnlyAfterTheirParseFailureFlowFileIsCommitted() throws Exception {
        useReader(new MockFailingRecordParser());
        final List<String> statesAtAcknowledgement = recordSessionStateAtAcknowledgement(ConsumePulsarRecord.REL_PARSE_FAILURE);
        mockClientService.setMockMessageQueue(messages(3));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 1);
        assertEquals("every unparseable message is acknowledged once its parse_failure FlowFile is committed",
                shared ? 3 : 1, statesAtAcknowledgement.size());
        for (final String state : statesAtAcknowledgement) {
            assertEquals("a message was acknowledged before the FlowFile carrying it was committed", ONE_COMMITTED_FLOWFILE, state);
        }
    }

    /**
     * The content repository rejects every write: neither the records nor the parse failures can be
     * persisted. Nothing may be acknowledged, so that the broker redelivers the batch. Before the fix the
     * messages were acknowledged anyway - before the write on a Shared subscription, after the failure on
     * an Exclusive one.
     */
    @Test
    public void nothingIsAcknowledgedWhenTheContentCannotBeWritten() throws Exception {
        useReader(parser());
        // schedule the processor against the still-empty topic
        runner.run(1, false, true);
        mockClientService.setMockMessageQueue(messages(3));
        final MockProcessSession session = sessionWhoseContentCannotBeWritten();

        ((ConsumePulsarRecord) runner.getProcessor()).onTrigger(runner.getProcessContext(), session);
        // what the framework does once onTrigger returns: it has to find a clean, rolled-back session
        session.commitAsync();
        // unschedule the processor, which waits for the acknowledgement pool to finish whatever was
        // submitted. (Running another trigger instead would consume what the failed one left in the
        // receiver queue - in synchronous mode the write fails on the first message - and acknowledge it.)
        ((AbstractPulsarConsumerProcessor<?>) runner.getProcessor()).shutDown(runner.getProcessContext());

        session.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        session.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        verifyNothingAcknowledged();
    }

    private static MockRecordParser parser() {
        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        return readerService;
    }

    private void useReader(final ControllerService readerService) throws InitializationException {
        runner.addControllerService("record-reader", readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
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
                .stream().allMatch(ConsumePulsarRecordAcknowledgementTest::isCommitted);

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

    /** One CSV record per message, parsed by MockRecordParser as (name, age). */
    private static List<Message<GenericRecord>> messages(final int count) {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();

        for (int n = 1; n <= count; n++) {
            msgs.add(new MockPulsarMessage<GenericRecord>(TOPIC, ("Name" + n + "," + n).getBytes(UTF_8), "1234:" + n + ":0", null, null));
        }

        return msgs;
    }
}
