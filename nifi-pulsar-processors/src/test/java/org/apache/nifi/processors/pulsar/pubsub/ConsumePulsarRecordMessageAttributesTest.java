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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class ConsumePulsarRecordMessageAttributesTest extends AbstractPulsarProcessorTest<GenericRecord> {

    @Mock
    private Consumer<GenericRecord> mockConsumer;

    @Mock
    private MessageId mockMessageId;

    private MockRecordParser recordParser;
    private MockRecordWriter recordWriter;

    @Before
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
        assertTrue("FlowFile should have pulsar.message.id attribute", 
                   flowFile.getAttributes().containsKey("pulsar.message.id"));
        assertEquals("Message ID should match", testMessageId, 
                     flowFile.getAttribute("pulsar.message.id"));

        // Verify all message properties are present with proper prefix
        assertTrue("FlowFile should have record-source property", 
                   flowFile.getAttributes().containsKey("pulsar.property.record-source"));
        assertEquals("Record-source property should match", "user-database", 
                     flowFile.getAttribute("pulsar.property.record-source"));

        assertTrue("FlowFile should have record-timestamp property", 
                   flowFile.getAttributes().containsKey("pulsar.property.record-timestamp"));
        assertEquals("Record-timestamp property should match", "2023-01-01T12:00:00Z", 
                     flowFile.getAttribute("pulsar.property.record-timestamp"));

        assertTrue("FlowFile should have record-version property", 
                   flowFile.getAttributes().containsKey("pulsar.property.record-version"));
        assertEquals("Record-version property should match", "v2.1", 
                     flowFile.getAttribute("pulsar.property.record-version"));

        // Verify record count attribute
        assertEquals("Record count should be 1", "1", flowFile.getAttribute("record.count"));
        
        // Verify topic name attribute is added
        assertEquals("Topic name should be present", "test-topic", flowFile.getAttribute("topicName"));
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
        
        String jsonData = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"},{\"id\":3,\"name\":\"Charlie\"}]";
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            jsonData.getBytes(),
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
        assertEquals("Message ID should match", testMessageId, 
                     flowFile.getAttribute("pulsar.message.id"));
        assertEquals("Batch-id property should match", "batch-001", 
                     flowFile.getAttribute("pulsar.property.batch-id"));
        assertEquals("Processing-time property should match", "2023-01-01T15:30:00Z", 
                     flowFile.getAttribute("pulsar.property.processing-time"));

        // Verify record count reflects all records processed
        assertEquals("Record count should be 3", "3", flowFile.getAttribute("record.count"));
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
        assertFalse("FlowFile should not have pulsar.message.id attribute when message ID is null", 
                    flowFile.getAttributes().containsKey("pulsar.message.id"));

        // Verify no property attributes are present (except topicName which is always added)
        for (String attributeName : flowFile.getAttributes().keySet()) {
            if (!attributeName.equals("topicName") && attributeName.startsWith("pulsar.property.")) {
                fail("No property attributes should be present besides topicName, but found: " + attributeName);
            }
        }

        // Verify basic attributes are still correct
        assertEquals("Record count should be 1", "1", flowFile.getAttribute("record.count"));
        assertEquals("Topic name should be present", "test-topic", flowFile.getAttribute("topicName"));
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
        assertEquals("Custom key mapping should work", messageKey, 
                     flowFile.getAttribute("user.key"));
        assertEquals("Custom property mapping should work", "crm", 
                     flowFile.getAttribute("system.source"));

        // Verify automatic attributes are also present
        assertEquals("Message ID should be present", testMessageId, 
                     flowFile.getAttribute("pulsar.message.id"));
        assertEquals("Source system property should be present with prefix", "crm", 
                     flowFile.getAttribute("pulsar.property.source-system"));
        assertEquals("Data classification property should be present with prefix", "sensitive", 
                     flowFile.getAttribute("pulsar.property.data-classification"));

        // Verify record processing worked correctly
        assertEquals("Record count should be 1", "1", flowFile.getAttribute("record.count"));
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
        assertTrue("Property with dashes should be present", 
                   flowFile.getAttributes().containsKey("pulsar.property.property-with-dashes"));
        assertEquals("Property with dashes should have correct value", "value1",
                     flowFile.getAttribute("pulsar.property.property-with-dashes"));
                     
        assertTrue("Property with dots should be present", 
                   flowFile.getAttributes().containsKey("pulsar.property.property.with.dots"));
        assertEquals("Property with dots should have correct value", "value2",
                     flowFile.getAttribute("pulsar.property.property.with.dots"));
                     
        assertTrue("Property with underscores should be present", 
                   flowFile.getAttributes().containsKey("pulsar.property.property_with_underscores"));
        assertEquals("Property with underscores should have correct value", "value3",
                     flowFile.getAttribute("pulsar.property.property_with_underscores"));
                     
        assertTrue("Property with spaces should be present", 
                   flowFile.getAttributes().containsKey("pulsar.property.property with spaces"));
        assertEquals("Property with spaces should have correct value", "value4",
                     flowFile.getAttribute("pulsar.property.property with spaces"));
    }
}