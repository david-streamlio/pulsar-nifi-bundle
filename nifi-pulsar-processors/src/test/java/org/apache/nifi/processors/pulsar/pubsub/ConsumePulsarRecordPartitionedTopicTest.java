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

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for issue #141: on a partitioned topic {@code Message#getTopicName()} reports the physical
 * partition, which used to be both the grouping key and part of the batch key. That split every batch into one
 * FlowFile per partition and re-emitted the messages in the hash order of the partition names.
 */
public class ConsumePulsarRecordPartitionedTopicTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String LOGICAL_TOPIC = "persistent://public/default/events";

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addPulsarClientService();

        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService("record-reader", readerService);
        runner.enableControllerService(readerService);

        final MockRecordWriter writerService = new MockRecordWriter("name, age");
        runner.addControllerService("record-writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, LOGICAL_TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "9");
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
    }

    /** The core of #141: 9 messages spread over 3 partitions of one topic must land in ONE FlowFile. */
    @Test
    public void messagesFromAllPartitionsOfOneTopicShareAFlowFile() {
        final List<Message<GenericRecord>> messages = new ArrayList<>();
        for (int n = 1; n <= 9; n++) {
            // round-robin across 3 partitions, exactly as a partitioned consumer delivers them
            messages.add(message(LOGICAL_TOPIC + "-partition-" + (n % 3), n));
        }
        mockClientService.setMockMessageQueue(messages);

        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS).get(0);

        assertEquals("9", flowFile.getAttribute(ConsumePulsarRecord.MSG_COUNT));
        // receive order preserved, not partition-name hash order
        flowFile.assertContentEquals(
                "\"Name1\",\"1\"\n\"Name2\",\"2\"\n\"Name3\",\"3\"\n"
                + "\"Name4\",\"4\"\n\"Name5\",\"5\"\n\"Name6\",\"6\"\n"
                + "\"Name7\",\"7\"\n\"Name8\",\"8\"\n\"Name9\",\"9\"\n");
        // the attribute reports the logical topic, not a physical partition
        flowFile.assertAttributeEquals("topicName", LOGICAL_TOPIC);
    }

    /** Guard against over-correcting: genuinely different topics must still not share a record set. */
    @Test
    public void messagesFromDifferentTopicsStillGoToSeparateFlowFiles() {
        mockClientService.setMockMessageQueue(java.util.Arrays.asList(
                message("persistent://public/default/orders-partition-0", 1),
                message("persistent://public/default/orders-partition-1", 2),
                message("persistent://public/default/shipments-partition-0", 3),
                message("persistent://public/default/shipments-partition-1", 4)));

        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);

        flowFiles.get(0).assertAttributeEquals("topicName", "persistent://public/default/orders");
        flowFiles.get(0).assertContentEquals("\"Name1\",\"1\"\n\"Name2\",\"2\"\n");
        flowFiles.get(1).assertAttributeEquals("topicName", "persistent://public/default/shipments");
        flowFiles.get(1).assertContentEquals("\"Name3\",\"3\"\n\"Name4\",\"4\"\n");
    }

    /** A non-partitioned topic must keep reporting exactly the name the broker gave us. */
    @Test
    public void nonPartitionedTopicNameIsUnchanged() {
        mockClientService.setMockMessageQueue(java.util.Arrays.asList(
                message(LOGICAL_TOPIC, 1), message(LOGICAL_TOPIC, 2)));

        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS).get(0)
                .assertAttributeEquals("topicName", LOGICAL_TOPIC);
    }

    // ------------------------------------------------------------------ getLogicalTopicName unit coverage

    @Test
    public void getLogicalTopicNameStripsThePartitionSuffix() {
        assertEquals(LOGICAL_TOPIC, ConsumePulsarRecord.getLogicalTopicName(LOGICAL_TOPIC + "-partition-0"));
        assertEquals(LOGICAL_TOPIC, ConsumePulsarRecord.getLogicalTopicName(LOGICAL_TOPIC + "-partition-42"));
    }

    @Test
    public void getLogicalTopicNameLeavesEverythingElseAlone() {
        // a non-partitioned topic keeps the exact string the broker reported, short form included
        assertEquals(LOGICAL_TOPIC, ConsumePulsarRecord.getLogicalTopicName(LOGICAL_TOPIC));
        assertEquals("foo", ConsumePulsarRecord.getLogicalTopicName("foo"));
        // malformed / absent names must not blow up the flow
        assertEquals("", ConsumePulsarRecord.getLogicalTopicName(""));
        assertEquals(null, ConsumePulsarRecord.getLogicalTopicName(null));
    }

    // ------------------------------------------------------------------ helpers

    /** One CSV record per message, parsed by MockRecordParser as (name, age). */
    private static Message<GenericRecord> message(final String topic, final int n) {
        return new MockPulsarMessage<GenericRecord>(topic, ("Name" + n + "," + n).getBytes(UTF_8),
                "1234:" + n + ":0", null, null);
    }
}
