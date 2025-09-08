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
package org.apache.nifi.processors.pulsar;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class AbstractPulsarConsumerProcessorMessageAttributesTest {

    @Mock
    private Message<GenericRecord> mockMessage;

    @Mock
    private MessageId mockMessageId;

    @Mock
    private PulsarClientService mockPulsarClientService;

    private TestConsumerProcessor processor;
    private TestRunner testRunner;

    @Before
    public void setUp() throws InitializationException {
        MockitoAnnotations.openMocks(this);
        processor = new TestConsumerProcessor();
        testRunner = TestRunners.newTestRunner(processor);
        
        // Set up minimal required properties
        testRunner.addControllerService("pulsar-client", mockPulsarClientService);
        testRunner.enableControllerService(mockPulsarClientService);
        testRunner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        testRunner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "test-topic");
        testRunner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "test-subscription");
    }

    @Test
    public void testMessageIdIsAddedToFlowFileAttributes() {
        // Setup mock message with message ID
        String expectedMessageId = "123:456:789";
        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessageId.toString()).thenReturn(expectedMessageId);
        when(mockMessage.getKey()).thenReturn(null);
        when(mockMessage.getProperties()).thenReturn(new HashMap<>());

        // Test the method
        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // Verify message ID is present
        assertTrue("Message ID should be present in attributes", attributes.containsKey("pulsar.message.id"));
        assertEquals("Message ID should match expected value", expectedMessageId, attributes.get("pulsar.message.id"));
    }

    @Test
    public void testMessagePropertiesAreAddedToFlowFileAttributes() {
        // Setup mock message with properties
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("source", "test-application");
        messageProperties.put("version", "1.2.3");
        messageProperties.put("environment", "production");

        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessageId.toString()).thenReturn("test-id");
        when(mockMessage.getKey()).thenReturn(null);
        when(mockMessage.getProperties()).thenReturn(messageProperties);

        // Test the method
        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // Verify all properties are present with proper prefix
        assertTrue("Source property should be present", attributes.containsKey("pulsar.property.source"));
        assertEquals("Source property should match", "test-application", attributes.get("pulsar.property.source"));

        assertTrue("Version property should be present", attributes.containsKey("pulsar.property.version"));
        assertEquals("Version property should match", "1.2.3", attributes.get("pulsar.property.version"));

        assertTrue("Environment property should be present", attributes.containsKey("pulsar.property.environment"));
        assertEquals("Environment property should match", "production", attributes.get("pulsar.property.environment"));
    }

    @Test
    public void testNullMessageIdHandledGracefully() {
        // Setup mock message with null message ID
        when(mockMessage.getMessageId()).thenReturn(null);
        when(mockMessage.getKey()).thenReturn(null);
        when(mockMessage.getProperties()).thenReturn(new HashMap<>());

        // Test the method
        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // Verify no message ID is added when null
        assertFalse("Message ID should not be present when null", attributes.containsKey("pulsar.message.id"));
    }

    @Test
    public void testEmptyPropertiesHandledGracefully() {
        // Setup mock message with empty properties
        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessageId.toString()).thenReturn("test-id");
        when(mockMessage.getKey()).thenReturn(null);
        when(mockMessage.getProperties()).thenReturn(new HashMap<>());

        // Test the method
        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // Verify message ID is still present
        assertTrue("Message ID should be present", attributes.containsKey("pulsar.message.id"));

        // Verify no property attributes are added
        for (String key : attributes.keySet()) {
            assertFalse("No property attributes should be present with empty properties", 
                       key.startsWith("pulsar.property."));
        }
    }

    @Test
    public void testNullPropertiesHandledGracefully() {
        // Setup mock message with null properties
        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessageId.toString()).thenReturn("test-id");
        when(mockMessage.getKey()).thenReturn(null);
        when(mockMessage.getProperties()).thenReturn(null);

        // Test the method
        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // Verify message ID is still present
        assertTrue("Message ID should be present", attributes.containsKey("pulsar.message.id"));

        // Verify no property attributes are added
        for (String key : attributes.keySet()) {
            assertFalse("No property attributes should be present with null properties", 
                       key.startsWith("pulsar.property."));
        }
    }

    @Test
    public void testMessageKeyAndCustomMappingsStillWork() {
        // Setup mock message with key and properties
        String messageKey = "test-key";
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("custom-prop", "custom-value");

        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessageId.toString()).thenReturn("test-id");
        when(mockMessage.getKey()).thenReturn(messageKey);
        when(mockMessage.getProperties()).thenReturn(messageProperties);
        when(mockMessage.getProperty("custom-prop")).thenReturn("custom-value");

        // Set up custom mapping for the key
        testRunner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "message.key=__KEY__,custom.attr=custom-prop");

        // Test the method
        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // Verify custom mappings still work
        assertTrue("Custom message key mapping should work", attributes.containsKey("message.key"));
        assertEquals("Message key should match", messageKey, attributes.get("message.key"));

        assertTrue("Custom property mapping should work", attributes.containsKey("custom.attr"));
        assertEquals("Custom property should match", "custom-value", attributes.get("custom.attr"));

        // Verify new automatic attributes are also present
        assertTrue("Message ID should be present", attributes.containsKey("pulsar.message.id"));
        assertTrue("Property should be present with prefix", attributes.containsKey("pulsar.property.custom-prop"));
    }

    @Test
    public void testSpecialCharactersInPropertyNames() {
        // Setup mock message with properties containing special characters
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("property-with-dashes", "value1");
        messageProperties.put("property.with.dots", "value2");
        messageProperties.put("property_with_underscores", "value3");
        messageProperties.put("property with spaces", "value4");

        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessageId.toString()).thenReturn("test-id");
        when(mockMessage.getKey()).thenReturn(null);
        when(mockMessage.getProperties()).thenReturn(messageProperties);

        // Test the method
        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // Verify all properties are preserved with their original names
        assertTrue("Property with dashes should be present", 
                   attributes.containsKey("pulsar.property.property-with-dashes"));
        assertTrue("Property with dots should be present", 
                   attributes.containsKey("pulsar.property.property.with.dots"));
        assertTrue("Property with underscores should be present", 
                   attributes.containsKey("pulsar.property.property_with_underscores"));
        assertTrue("Property with spaces should be present", 
                   attributes.containsKey("pulsar.property.property with spaces"));
    }

    // Test processor that extends AbstractPulsarConsumerProcessor for testing purposes
    private static class TestConsumerProcessor extends AbstractPulsarConsumerProcessor<byte[]> {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            // No-op for testing
        }

        // Expose protected method for testing
        public Map<String, String> testGetMappedFlowFileAttributes(ProcessContext context, Message<GenericRecord> msg) {
            return getMappedFlowFileAttributes(context, msg);
        }
    }
}