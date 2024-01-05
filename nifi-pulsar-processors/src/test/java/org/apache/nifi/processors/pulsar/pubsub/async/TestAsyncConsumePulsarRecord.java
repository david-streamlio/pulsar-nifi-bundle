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
package org.apache.nifi.processors.pulsar.pubsub.async;

import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.TestConsumePulsarRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestAsyncConsumePulsarRecord extends TestConsumePulsarRecord {

    @Test
    public void emptyMessageTest() throws PulsarClientException {
        when(mockMessage.getData()).thenReturn("".getBytes());
        when(mockMessage.getTopicName()).thenReturn(DEFAULT_TOPIC);

        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(ConsumePulsarRecord.CONSUMER_BATCH_SIZE, 1 + "");
        runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(true));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_PARSE_FAILURE);

        verify(mockClientService.getMockConsumer(), times(1)).acknowledgeCumulativeAsync(mockMessage);
    }

    @Test
    public void singleMalformedMessageTest() throws PulsarClientException {
        when(mockMessage.getData()).thenReturn(BAD_MSG.getBytes());
        when(mockMessage.getTopicName()).thenReturn(DEFAULT_TOPIC);
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(ConsumePulsarRecord.CONSUMER_BATCH_SIZE, 1 + "");
        runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(true));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_PARSE_FAILURE);

        verify(mockClientService.getMockConsumer(), times(1)).acknowledgeCumulativeAsync(mockMessage);
    }

    /*
     * Send a single message containing a single record
     */
    @Test
    public void singleMessageTest() throws PulsarClientException {
        this.sendMessages(MOCKED_MSG, true, 1);
    }

    /*
     * Send a single message with multiple records
     */
    @Test
    public void singleMessageMultiRecordsTest() throws PulsarClientException {
        StringBuffer input = new StringBuffer(1024);
        StringBuffer expected = new StringBuffer(1024);

        for (int idx = 0; idx < 50; idx++) {
            input.append("Justin Thyme, " + idx).append("\n");
            expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
        }

        List<MockFlowFile> results = this.sendMessages(input.toString(), false, 1);

        String flowFileContents = new String(runner.getContentAsByteArray(results.get(0)));
        assertEquals(expected.toString(), flowFileContents);
    }

    /*
     * Send multiple messages on different topics,
     * check if it creates two flow files by retaining the order of messages
     */
    @Test
    public void multipleGoodMessagesOnTwoTopicsCreatesMultipleRecordsTest() throws IOException {

        List<Message<GenericRecord>> mockMessages = new ArrayList<>();
        mockMessages.add(createTestMessage("A,9".getBytes(), null, singletonMap("prop", "val"), DEFAULT_TOPIC, "", SchemaType.JSON));
        mockMessages.add(createTestMessage("Z,10".getBytes(), null, singletonMap("prop", "val"), DEFAULT_TOPIC_2, "", SchemaType.JSON));
        mockMessages.add(createTestMessage("G,1".getBytes(), null, singletonMap("prop", "val"), DEFAULT_TOPIC, "", SchemaType.JSON));
        mockMessages.add(createTestMessage("F,7".getBytes(), "K", singletonMap("prop", "val"), DEFAULT_TOPIC_2, "", SchemaType.JSON));

        mockClientService.setMockMessages(mockMessages);

        runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(false));
        runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC + "," + DEFAULT_TOPIC_2);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(ConsumePulsarRecord.CONSUMER_BATCH_SIZE, 4 + "");
        runner.run(1, true);

        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        successFlowFiles.get(0).assertContentEquals("\"Z\",\"10\"\n\"F\",\"7\"\n".getBytes());
        successFlowFiles.get(0).assertAttributeNotExists("avro.schema");
        successFlowFiles.get(1).assertContentEquals("\"A\",\"9\"\n\"G\",\"1\"\n".getBytes());
        successFlowFiles.get(1).assertAttributeNotExists("avro.schema");
        assertEquals(2, successFlowFiles.size());
    }

    /*
     * Send multiple messages on different topics while updating schema for one topic,
     * check if it creates two three files by retaining the order of messages
     */
    @Test
    public void multipleGoodMessagesWithSchemaUpdateOnTwoTopicsTest() throws IOException {
        String schema1 = "{\"type\": \"record\",\"name\": \"ExampleRecord\",\"fields\": [{\"name\": \"field1\", \"type\": \"int\"}]}\r\n";
        String schema1_updated = "{\"type\": \"record\",\"name\": \"ExampleRecord\",\"fields\": [{\"name\": \"field1\", \"type\": \"int\"},{\"name\": \"field2\", \"type\": [\"null\", \"string\"], \"default\": null}]}\r\n";

        List<Message<GenericRecord>> mockMessages = new ArrayList<>();
        mockMessages.add(createTestMessage("A,9".getBytes(), null, singletonMap("prop", "val"), DEFAULT_TOPIC, schema1, SchemaType.AVRO));
        mockMessages.add(createTestMessage("Z,10".getBytes(), null, singletonMap("prop", "val"), DEFAULT_TOPIC_2, schema1, SchemaType.AVRO));
        mockMessages.add(createTestMessage("G,1".getBytes(), null, singletonMap("prop", "val"), DEFAULT_TOPIC, schema1_updated, SchemaType.AVRO));
        mockMessages.add(createTestMessage("F,7".getBytes(), "K", singletonMap("prop", "val"), DEFAULT_TOPIC_2, schema1, SchemaType.AVRO));

        mockClientService.setMockMessages(mockMessages);

        runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(false));
        runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC + "," + DEFAULT_TOPIC_2);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(ConsumePulsarRecord.CONSUMER_BATCH_SIZE, 4 + "");
        runner.run(1, true);

        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        successFlowFiles.get(0).assertContentEquals("\"Z\",\"10\"\n\"F\",\"7\"\n".getBytes());
        successFlowFiles.get(0).assertAttributeExists("avro.schema");
        successFlowFiles.get(0).assertAttributeEquals("avro.schema", schema1);
        successFlowFiles.get(1).assertContentEquals("\"A\",\"9\"\n".getBytes());
        successFlowFiles.get(1).assertAttributeExists("avro.schema");
        successFlowFiles.get(1).assertAttributeEquals("avro.schema", schema1);
        successFlowFiles.get(2).assertContentEquals("\"G\",\"1\"\n".getBytes());
        successFlowFiles.get(2).assertAttributeExists("avro.schema");
        successFlowFiles.get(2).assertAttributeEquals("avro.schema", schema1_updated);
        assertEquals(3, successFlowFiles.size());
    }

    /*
     * Send a single message with multiple records,
     * some of them good and some malformed
     */
    @Test
    public void singleMessageWithGoodAndBadRecordsTest() throws PulsarClientException {
        StringBuffer input = new StringBuffer(1024);
        StringBuffer expected = new StringBuffer(1024);

        for (int idx = 0; idx < 10; idx++) {
            if (idx % 2 == 0) {
                input.append("Justin Thyme, " + idx).append("\n");
                expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
            } else {
                input.append(BAD_MSG).append("\n");
            }
        }

        when(mockMessage.getData()).thenReturn(input.toString().getBytes());
        when(mockMessage.getTopicName()).thenReturn(DEFAULT_TOPIC);
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(false));
        runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(ConsumePulsarRecord.CONSUMER_BATCH_SIZE, 1 + "");
        runner.run(1, true);

        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        assertEquals(1, successFlowFiles.size());

        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE);
        assertEquals(1, failureFlowFiles.size());
    }

    /*
     * Send multiple messages with Multiple records each
     */
    @Test
    public void multipleMultiRecordsTest() throws PulsarClientException {
        doMultipleMultiRecordsTest("Exclusive");
    }

    @Test
    public void multipleMultiRecordsSharedSubTest() throws PulsarClientException {
        doMultipleMultiRecordsTest("Shared");
    }

    @Test
    public void parseFailuresTest() throws Exception {
        doFailedParseHandlingTest("message", "topic", "sub", true);
    }

    private void doMultipleMultiRecordsTest(String subType) throws PulsarClientException {
        StringBuffer input = new StringBuffer(1024);
        StringBuffer expected = new StringBuffer(1024);

        for (int idx = 0; idx < 50; idx++) {
            input.append("Justin Thyme, " + idx).append("\n");
            expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
        }

        List<MockFlowFile> results = this.sendMessages(input.toString(), false, 50, 100, subType);
        assertEquals(50, results.size());

        String flowFileContents = new String(runner.getContentAsByteArray(results.get(0)));
        assertTrue(flowFileContents.startsWith(expected.toString(), 0));
    }

    @Test
    public void mappedAttributesTest() throws PulsarClientException {
        runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(true));

        super.doMappedAttributesTest();
    }

    private static Message<GenericRecord> createTestMessage(byte[] data,
                                                            String key,
                                                            Map<String, String> properties,
                                                            String topicName,
                                                            String schemaString,
                                                            SchemaType schemaType
    ) {
        Message mockA = mock(Message.class);
        Schema<Object> schema = mock(Schema.class);
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        when(mockA.getReaderSchema()).thenReturn(Optional.of(schema));
        when(schema.getSchemaInfo()).thenReturn(schemaInfo);
        when(schemaInfo.getType()).thenReturn(schemaType);
        when(schemaInfo.getSchema()).thenReturn(schemaString.getBytes());
        when(mockA.getData()).thenReturn(data);
        properties.entrySet().forEach(e ->
                when(mockA.getProperty(e.getKey())).thenReturn(e.getValue())
        );
        when(mockA.getTopicName()).thenReturn(topicName);
        when(mockA.getKey()).thenReturn(key);
        return mockA;
    }
}
