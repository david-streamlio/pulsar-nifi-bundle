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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
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
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumePulsarMessageAttributesTest extends AbstractPulsarProcessorTest<GenericRecord> {

    @Mock
    private Consumer<GenericRecord> mockConsumer;

    @Mock
    private MessageId mockMessageId;

    @BeforeEach
    public void init() throws Exception {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();

        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "test-topic");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "test-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "1");
        
        // Mock the consumer creation - remove these lines as they're handled by MockPulsarClientService
    }

    @Test
    public void testConsumePulsarAddsMessageIdAndPropertiesToFlowFile() throws PulsarClientException {
        // Create a mock message with ID and properties
        String testMessageId = "1234:5678:90";
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("source", "test-app");
        messageProperties.put("timestamp", "2023-01-01T10:00:00Z");
        messageProperties.put("correlation-id", "abc-123");
        
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            "Hello Pulsar".getBytes(),
            testMessageId,
            messageProperties,
            null // key
        );

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify message ID attribute
        assertTrue(flowFile.getAttributes().containsKey("pulsar.message.id"), 
                   "FlowFile should have pulsar.message.id attribute");
        assertEquals(testMessageId, flowFile.getAttribute("pulsar.message.id"), 
                     "Message ID should match");

        // Verify all message properties are present with proper prefix
        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.source"), 
                   "FlowFile should have source property");
        assertEquals("test-app", flowFile.getAttribute("pulsar.property.source"), 
                     "Source property should match");

        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.timestamp"), 
                   "FlowFile should have timestamp property");
        assertEquals("2023-01-01T10:00:00Z", flowFile.getAttribute("pulsar.property.timestamp"), 
                     "Timestamp property should match");

        assertTrue(flowFile.getAttributes().containsKey("pulsar.property.correlation-id"), 
                   "FlowFile should have correlation-id property");
        assertEquals("abc-123", flowFile.getAttribute("pulsar.property.correlation-id"), 
                     "Correlation-id property should match");

        // Verify content is correct
        flowFile.assertContentEquals("Hello Pulsar");
        
        // Verify message count attribute
        assertEquals("1", flowFile.getAttribute("message.count"), "Message count should be 1");
    }

    @Test
    public void testConsumePulsarHandlesMessageWithoutPropertiesOrId() throws PulsarClientException {
        // Create a mock message with no properties and no ID
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            "Hello Pulsar".getBytes(),
            null, // no message ID
            null, // no properties
            null  // no key
        );

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify message ID attribute is not present when null
        assertFalse(flowFile.getAttributes().containsKey("pulsar.message.id"), 
                    "FlowFile should not have pulsar.message.id attribute when message ID is null");

        // Verify no property attributes are present
        for (String attributeName : flowFile.getAttributes().keySet()) {
            assertFalse(attributeName.startsWith("pulsar.property."), 
                       "No property attributes should be present");
        }

        // Verify content and basic attributes are still correct
        flowFile.assertContentEquals("Hello Pulsar");
        assertEquals("1", flowFile.getAttribute("message.count"), "Message count should be 1");
    }

    @Test
    public void testConsumePulsarWithMessageKeyAndCustomMapping() throws PulsarClientException {
        // Create a mock message with key, ID, and properties
        String messageKey = "user-123";
        String testMessageId = "msg-456";
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("user-type", "premium");
        
        Message<GenericRecord> mockMessage = new MockPulsarMessage<>(
            "test-topic", 
            "User data".getBytes(),
            testMessageId,
            messageProperties,
            messageKey
        );

        // Set up custom attribute mapping
        runner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, 
                          "message.key=__KEY__,user.type=user-type");

        // Set up the mock message in the client service
        mockClientService.setMockMessage(mockMessage);

        // Run the processor
        runner.run(1);

        // Verify a FlowFile was created
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        // Verify custom mappings work
        assertEquals(messageKey, flowFile.getAttribute("message.key"), 
                     "Custom key mapping should work");
        assertEquals("premium", flowFile.getAttribute("user.type"), 
                     "Custom property mapping should work");

        // Verify automatic attributes are also present
        assertEquals(testMessageId, flowFile.getAttribute("pulsar.message.id"), 
                     "Message ID should be present");
        assertEquals("premium", flowFile.getAttribute("pulsar.property.user-type"), 
                     "Property should be present with prefix");

        // Verify content
        flowFile.assertContentEquals("User data");
    }

    @Test
    public void testConsumePulsarWithMultipleMessages() throws PulsarClientException {
        // Create multiple mock messages with different properties
        String messageId1 = "msg-001";
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("batch", "1");
        properties1.put("sequence", "first");

        String messageId2 = "msg-002";
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("batch", "1");
        properties2.put("sequence", "second");

        Message<GenericRecord> mockMessage1 = new MockPulsarMessage<>(
            "test-topic", 
            "Message 1".getBytes(),
            messageId1,
            properties1,
            null
        );

        Message<GenericRecord> mockMessage2 = new MockPulsarMessage<>(
            "test-topic", 
            "Message 2".getBytes(),
            messageId2,
            properties2,
            null
        );

        // Set batch size to allow multiple messages
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "2");

        // Set up multiple messages in the client service
        java.util.List<Message<GenericRecord>> messages = java.util.Arrays.asList(mockMessage1, mockMessage2);
        mockClientService.setMockMessages(messages);

        // Run the processor
        runner.run(1);

        // The two messages have different ids and a different "sequence" property, but neither is a
        // mapped attribute: per-message metadata must never split a batch. Both messages therefore land
        // in ONE FlowFile (Consumer Message Batch Size = 2) whose metadata attributes describe the batch.
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("Message 1\nMessage 2");
        assertEquals("2", flowFile.getAttribute("message.count"), "Message count should be 2");

        // No single message id describes a two-message FlowFile: only the first/last ids are published
        flowFile.assertAttributeNotExists("pulsar.message.id");
        assertEquals(messageId1, flowFile.getAttribute("pulsar.message.id.first"),
                     "First message id should be from the first message");
        assertEquals(messageId2, flowFile.getAttribute("pulsar.message.id.last"),
                     "Last message id should be from the last message");

        // Properties shared by every message in the batch are kept, the differing one is dropped
        assertEquals("1", flowFile.getAttribute("pulsar.property.batch"),
                     "Common batch property should be present");
        flowFile.assertAttributeNotExists("pulsar.property.sequence");
    }
}