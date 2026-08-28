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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

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
 * When no record set is ever opened - every message in the batch fails to resolve a schema, so each one
 * takes the parse-failure path - ConsumePulsarRecord used to call finishRecordSet() on a null writer and
 * throw a NullPointerException out of onTrigger, losing the whole session (and with it the parse failures)
 * even though the messages had already been acknowledged.
 */
public class ConsumePulsarRecordNoRecordSetTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/events";

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
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
    }

    @Test
    public void everyMessageFailingToResolveASchemaDoesNotThrow() {
        mockClientService.setMockMessageQueue(messages(3));

        // Before the fix this threw NullPointerException out of onTrigger.
        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        // the messages are not silently dropped: they are routed for inspection
        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 1);
        assertTrue("the unparseable payloads should reach the parse_failure relationship",
                new String(runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE)
                        .get(0).toByteArray(), UTF_8).contains("Name1"));
    }

    @Test
    public void aSingleUnparseableMessageDoesNotThrow() {
        mockClientService.setMockMessageQueue(messages(1));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 1);
    }

    private static List<Message<GenericRecord>> messages(final int count) {
        final Message<GenericRecord>[] msgs = new Message[count];
        for (int n = 1; n <= count; n++) {
            msgs[n - 1] = new MockPulsarMessage<GenericRecord>(TOPIC, ("Name" + n + "," + n).getBytes(UTF_8),
                    "1234:" + n + ":0", null, null);
        }
        return Arrays.asList(msgs);
    }
}
