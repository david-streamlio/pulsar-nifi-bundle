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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockFailingRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * The parse-failure path must leave the session clean even when it cannot write.
 * <p>
 * {@code handleFailures()} created a FlowFile, opened a stream on it, and on IOException logged and
 * returned - leaving the stream open and the FlowFile neither transferred nor removed. The messages that
 * failed to parse were lost and the session was left in a state it could not be committed from. This is
 * the same shape as the crash fixed in #147, in the error path of the same class.
 */
public class ConsumePulsarRecordFailureRoutingTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/failure-routing";

    /** A processor whose parse-failure write always fails, so the error path is actually taken. */
    public static class FailingWriteConsumePulsarRecord extends ConsumePulsarRecord {
        @Override
        protected void writeParseFailures(final OutputStream out,
                                          final BlockingQueue<Message<GenericRecord>> parseFailures,
                                          final byte[] demarcator) throws IOException {
            throw new IOException("Intentional Unit Test Exception while writing parse failures");
        }
    }

    private void configure() throws InitializationException {
        addPulsarClientService();

        final MockFailingRecordParser readerService = new MockFailingRecordParser();
        runner.addControllerService("record-reader", readerService);
        runner.enableControllerService(readerService);

        final MockRecordWriter writerService = new MockRecordWriter("name, age");
        runner.addControllerService("record-writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "3");
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");

        mockClientService.setMockMessageQueue(messages());
    }

    @Before
    public void init() {
        // each test picks its own processor class
    }

    /**
     * The healthy case: unparseable messages reach parse_failure. This is the behaviour the error-path
     * fix must not disturb.
     */
    @Test
    public void unparseableMessagesAreRoutedToParseFailure() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        configure();

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 1);
    }

    /**
     * When the write itself fails, the FlowFile must be discarded rather than left dangling. Before the
     * fix the stream stayed open and the FlowFile was neither transferred nor removed, so committing the
     * session threw and the whole trigger was lost.
     */
    @Test
    public void aFailedWriteLeavesNoDanglingFlowFile() throws InitializationException {
        runner = TestRunners.newTestRunner(FailingWriteConsumePulsarRecord.class);
        configure();

        // Before the fix this threw from session commit: a FlowFile was created, written to and then
        // abandoned with its OutputStream still open.
        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
    }

    private static List<Message<GenericRecord>> messages() {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();
        for (int n = 1; n <= 3; n++) {
            msgs.add(new MockPulsarMessage<GenericRecord>(TOPIC, ("Name" + n + "," + n).getBytes(UTF_8),
                    "1234:" + n + ":0", null, null));
        }
        return msgs;
    }
}
