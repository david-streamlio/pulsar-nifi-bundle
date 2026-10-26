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

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumePulsarRecordMessageAttributesTest extends AbstractPulsarProcessorTest<GenericRecord> {

    @Mock
    private Consumer<GenericRecord> mockConsumer;

    @Mock
    private MessageId mockMessageId;

    private MockRecordParser recordParser;
    private MockRecordWriter recordWriter;

    @BeforeEach
    public void init() throws Exception {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addPulsarClientService();

        // Set up record reader and writer
        recordParser = new MockRecordParser();
        recordWriter = new MockRecordWriter("header", false);
        
        runner.addControllerService("record-reader", recordParser);
        runner.enableControllerService(recordParser);
        runner.addControllerService("record-writer", recordWriter);
        runner.enableControllerService(recordWriter);

        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "test-topic");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "test-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        
        // Mock the consumer creation - remove these lines as they're handled by MockPulsarClientService
    }

    @Test
    public void testConsumePulsarRecordAddsMessageIdAndPropertiesToFlowFile() throws PulsarClientException {
        // Set up record parser with test data
        recordParser.addSchemaField("name", RecordFieldType.STRING);
        recordParser.addSchemaField("age", RecordFieldType.INT);
        recordParser.addRecord("John Doe", 30);

        // Create a mock message with ID and properties
        String testMessageId = "record-msg-123";
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("record-source", "user-database");
        messageProperties.put("record-timestamp", "2023-01-01T12:00:00Z");
        messageProperties.put("record-version", "v2.1");
        
        String jsonData = "{\"name\":\"John Doe\",\"age\":30}";
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            jsonData.getBytes(),
            testMessageId,
            messageProperties,
            null // key
        );

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify message ID attribute
        assertTrue(flowFile.getAttributes().containsKey("pulsar.message.id"), 
                   "FlowFile should have pulsar.message.id attribute");
        assertEquals(testMessageId, flowFile.getAttribute("pulsar.message.id"), 
                     "Message ID should match");

        // Verify all message properties are present with proper prefix
        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.record-source"), 
                   "FlowFile should have record-source property");
        assertEquals("user-database", flowFile.getAttribute("pulsar.property.record-source"), 
                     "Record-source property should match");

        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.record-timestamp"), 
                   "FlowFile should have record-timestamp property");
        assertEquals("2023-01-01T12:00:00Z", flowFile.getAttribute("pulsar.property.record-timestamp"), 
                     "Record-timestamp property should match");

        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.record-version"), 
                   "FlowFile should have record-version property");
        assertEquals("v2.1", flowFile.getAttribute("pulsar.property.record-version"), 
                     "Record-version property should match");

        // Verify record count attribute
        assertEquals("1", flowFile.getAttribute("record.count"), "Record count should be 1");
        
        // Verify topic name attribute is added
        assertEquals("test-topic", flowFile.getAttribute("topicName"), "Topic name should be present");
    }

    @Test
    public void testConsumePulsarRecordWithMultipleRecordsInMessage() throws PulsarClientException {
        // Set up record parser with multiple records
        recordParser.addSchemaField("id", RecordFieldType.INT);
        recordParser.addSchemaField("name", RecordFieldType.STRING);
        recordParser.addRecord(1, "Alice");
        recordParser.addRecord(2, "Bob");
        recordParser.addRecord(3, "Charlie");

        // Create a mock message with properties
        String testMessageId = "multi-record-msg-456";
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("batch-id", "batch-001");
        messageProperties.put("processing-time", "2023-01-01T15:30:00Z");
        
        // MockRecordParser parses the message content line-by-line (one record per line),
        // splitting each line on commas to populate the configured schema fields
        // (id, name). To exercise multiple records in a single message, the content must
        // contain three lines matching that schema.
        String recordData = "1,Alice\n2,Bob\n3,Charlie";
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic",
            recordData.getBytes(),
            testMessageId,
            messageProperties,
            "user-batch-key"
        );

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify message ID and properties are present
        assertEquals(testMessageId, flowFile.getAttribute("pulsar.message.id"), 
                     "Message ID should match");
        assertEquals("batch-001", flowFile.getAttribute("pulsar.property.batch-id"), 
                     "Batch-id property should match");
        assertEquals("2023-01-01T15:30:00Z", flowFile.getAttribute("pulsar.property.processing-time"), 
                     "Processing-time property should match");

        // Verify record count reflects all records processed
        assertEquals("3", flowFile.getAttribute("record.count"), "Record count should be 3");
    }

    @Test
    public void testConsumePulsarRecordHandlesMessageWithoutPropertiesOrId() throws PulsarClientException {
        // Set up record parser
        recordParser.addSchemaField("value", RecordFieldType.STRING);
        recordParser.addRecord("test-value");

        // Create a mock message with no properties and no ID
        String jsonData = "{\"value\":\"test-value\"}";
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            jsonData.getBytes(),
            null, // no message ID
            null, // no properties
            null  // no key
        );

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify message ID attribute is not present when null
        assertFalse(flowFile.getAttributes().containsKey("pulsar.message.id"), 
                    "FlowFile should not have pulsar.message.id attribute when message ID is null");

        // Verify no property attributes are present (except topicName which is always added)
        for (String attributeName : flowFile.getAttributes().keySet()) {
            if (!attributeName.equals("topicName") && attributeName.startsWith("pulsar.property.")) {
                fail("No property attributes should be present besides topicName, but found: " + attributeName);
            }
        }

        // Verify basic attributes are still correct
        assertEquals("1", flowFile.getAttribute("record.count"), "Record count should be 1");
        assertEquals("test-topic", flowFile.getAttribute("topicName"), "Topic name should be present");
    }

    @Test
    public void testConsumePulsarRecordWithCustomMappings() throws PulsarClientException {
        // Set up record parser
        recordParser.addSchemaField("user_id", RecordFieldType.STRING);
        recordParser.addSchemaField("email", RecordFieldType.STRING);
        recordParser.addRecord("user123", "user@example.com");

        // Create a mock message with key and properties
        String messageKey = "user-key-789";
        String testMessageId = "custom-msg-789";
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("source-system", "crm");
        messageProperties.put("data-classification", "sensitive");
        
        String jsonData = "{\"user_id\":\"user123\",\"email\":\"user@example.com\"}";
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            jsonData.getBytes(),
            testMessageId,
            messageProperties,
            messageKey
        );

        // Set up custom attribute mapping
        runner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, 
                          "user.key=__KEY__,system.source=source-system");

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify custom mappings work
        assertEquals(messageKey, flowFile.getAttribute("user.key"), 
                     "Custom key mapping should work");
        assertEquals("crm", flowFile.getAttribute("system.source"), 
                     "Custom property mapping should work");

        // Verify automatic attributes are also present
        assertEquals(testMessageId, flowFile.getAttribute("pulsar.message.id"), 
                     "Message ID should be present");
        assertEquals("crm", flowFile.getAttribute("pulsar.property.source-system"), 
                     "Source system property should be present with prefix");
        assertEquals("sensitive", flowFile.getAttribute("pulsar.property.data-classification"), 
                     "Data classification property should be present with prefix");

        // Verify record processing worked correctly
        assertEquals("1", flowFile.getAttribute("record.count"), "Record count should be 1");
    }

    @Test
    public void testConsumePulsarRecordPreservesSpecialCharactersInProperties() throws PulsarClientException {
        // Set up record parser
        recordParser.addSchemaField("data", RecordFieldType.STRING);
        recordParser.addRecord("test data");

        // Create properties with special characters
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("property-with-dashes", "value1");
        messageProperties.put("property.with.dots", "value2");  
        messageProperties.put("property_with_underscores", "value3");
        messageProperties.put("property with spaces", "value4");

        String jsonData = "{\"data\":\"test data\"}";
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            jsonData.getBytes(),
            "special-chars-msg",
            messageProperties,
            null
        );

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify all properties are preserved with their original names
        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.property-with-dashes"), 
                   "Property with dashes should be present");
        assertEquals("value1", flowFile.getAttribute("pulsar.property.property-with-dashes"),
                     "Property with dashes should have correct value");
                     
        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.property.with.dots"), 
                   "Property with dots should be present");
        assertEquals("value2", flowFile.getAttribute("pulsar.property.property.with.dots"),
                     "Property with dots should have correct value");
                     
        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.property_with_underscores"), 
                   "Property with underscores should be present");
        assertEquals("value3", flowFile.getAttribute("pulsar.property.property_with_underscores"),
                     "Property with underscores should have correct value");
                     
        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.property with spaces"), 
                   "Property with spaces should be present");
        assertEquals("value4", flowFile.getAttribute("pulsar.property.property with spaces"),
                     "Property with spaces should have correct value");
    }
}