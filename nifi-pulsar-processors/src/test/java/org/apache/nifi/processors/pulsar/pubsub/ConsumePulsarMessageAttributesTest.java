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
import static org.mockito.Mockito.*;

public class ConsumePulsarMessageAttributesTest extends AbstractPulsarProcessorTest<GenericRecord> {

    @Mock
    private Consumer<GenericRecord> mockConsumer;

    @Mock
    private MessageId mockMessageId;

    @Before
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
        assertTrue("FlowFile should have pulsar.message.id attribute", 
                   flowFile.getAttributes().containsKey("pulsar.message.id"));
        assertEquals("Message ID should match", testMessageId, 
                     flowFile.getAttribute("pulsar.message.id"));

        // Verify all message properties are present with proper prefix
        assertTrue("FlowFile should have source property", 
                   flowFile.getAttributes().containsKey("pulsar.property.source"));
        assertEquals("Source property should match", "test-app", 
                     flowFile.getAttribute("pulsar.property.source"));

        assertTrue("FlowFile should have timestamp property", 
                   flowFile.getAttributes().containsKey("pulsar.property.timestamp"));
        assertEquals("Timestamp property should match", "2023-01-01T10:00:00Z", 
                     flowFile.getAttribute("pulsar.property.timestamp"));

        assertTrue("FlowFile should have correlation-id property", 
                   flowFile.getAttributes().containsKey("pulsar.property.correlation-id"));
        assertEquals("Correlation-id property should match", "abc-123", 
                     flowFile.getAttribute("pulsar.property.correlation-id"));

        // Verify content is correct
        flowFile.assertContentEquals("Hello Pulsar");
        
        // Verify message count attribute
        assertEquals("Message count should be 1", "1", flowFile.getAttribute("message.count"));
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
        assertFalse("FlowFile should not have pulsar.message.id attribute when message ID is null", 
                    flowFile.getAttributes().containsKey("pulsar.message.id"));

        // Verify no property attributes are present
        for (String attributeName : flowFile.getAttributes().keySet()) {
            assertFalse("No property attributes should be present", 
                       attributeName.startsWith("pulsar.property."));
        }

        // Verify content and basic attributes are still correct
        flowFile.assertContentEquals("Hello Pulsar");
        assertEquals("Message count should be 1", "1", flowFile.getAttribute("message.count"));
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
        assertEquals("Custom key mapping should work", messageKey, 
                     flowFile.getAttribute("message.key"));
        assertEquals("Custom property mapping should work", "premium", 
                     flowFile.getAttribute("user.type"));

        // Verify automatic attributes are also present
        assertEquals("Message ID should be present", testMessageId, 
                     flowFile.getAttribute("pulsar.message.id"));
        assertEquals("Property should be present with prefix", "premium", 
                     flowFile.getAttribute("pulsar.property.user-type"));

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

        // The two messages carry different "sequence" property values, so they map to
        // different FlowFile attribute sets. The consumer only concatenates consecutive
        // messages that share identical mapped attributes into a single FlowFile;
        // messages with differing attributes are emitted as separate FlowFiles.
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);

        MockFlowFile firstFlowFile = flowFiles.get(0);
        MockFlowFile secondFlowFile = flowFiles.get(1);

        // First FlowFile corresponds to the first message
        assertEquals("First FlowFile message ID should match first message", messageId1,
                     firstFlowFile.getAttribute("pulsar.message.id"));
        assertEquals("First FlowFile batch property should be present", "1",
                     firstFlowFile.getAttribute("pulsar.property.batch"));
        assertEquals("First FlowFile sequence property should match first message", "first",
                     firstFlowFile.getAttribute("pulsar.property.sequence"));
        firstFlowFile.assertContentEquals("Message 1");
        assertEquals("First FlowFile message count should be 1", "1",
                     firstFlowFile.getAttribute("message.count"));

        // Second FlowFile corresponds to the second message
        assertEquals("Second FlowFile message ID should match second message", messageId2,
                     secondFlowFile.getAttribute("pulsar.message.id"));
        assertEquals("Second FlowFile batch property should be present", "1",
                     secondFlowFile.getAttribute("pulsar.property.batch"));
        assertEquals("Second FlowFile sequence property should match second message", "second",
                     secondFlowFile.getAttribute("pulsar.property.sequence"));
        secondFlowFile.assertContentEquals("Message 2");
        assertEquals("Second FlowFile message count should be 1", "1",
                     secondFlowFile.getAttribute("message.count"));
    }
}