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
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarClientService;
import org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Verifies the contract of {@link AbstractPulsarConsumerProcessor#getMappedFlowFileAttributes}: its result is
 * the set of attributes that decides whether consecutive messages may share a FlowFile, so it must contain the
 * user-configured "Mapped FlowFile Attributes" only. Per-message metadata (message id, message properties) is
 * published through {@link MessageBatchAttributes} instead and must never leak into this map; if it did, every
 * message would start a new FlowFile and "Consumer Message Batch Size" would be defeated.
 */
public class AbstractPulsarConsumerProcessorMessageAttributesTest {

    @Mock
    private Message<GenericRecord> mockMessage;

    @Mock
    private MessageId mockMessageId;

    // Use the concrete MockPulsarClientService (a real AbstractControllerService)
    // rather than a bare Mockito interface mock, because TestRunner.addControllerService
    // requires an actual ControllerService implementation.
    private MockPulsarClientService<byte[]> mockPulsarClientService;

    private TestConsumerProcessor processor;
    private TestRunner testRunner;

    @Before
    public void setUp() throws InitializationException {
        MockitoAnnotations.openMocks(this);
        processor = new TestConsumerProcessor();
        testRunner = TestRunners.newTestRunner(processor);
        mockPulsarClientService = new MockPulsarClientService<>();

        // Set up minimal required properties
        testRunner.addControllerService("pulsar-client", mockPulsarClientService);
        testRunner.enableControllerService(mockPulsarClientService);
        testRunner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        testRunner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "test-topic");
        testRunner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "test-subscription");

        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessageId.toString()).thenReturn("123:456:789");
    }

    private void givenMessageProperties(final Map<String, String> properties) {
        when(mockMessage.getProperties()).thenReturn(properties);
        when(mockMessage.getProperty(anyString())).thenAnswer(invocation -> properties.get(invocation.<String>getArgument(0)));
    }

    @Test
    public void testMessageIdIsNotPartOfMappedAttributes() {
        givenMessageProperties(new HashMap<>());

        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        assertFalse("The message id changes with every message and must not take part in the batch comparison",
                attributes.containsKey(MessageBatchAttributes.MESSAGE_ID_ATTRIBUTE));
        assertTrue("Without mappings the batch key must be empty but was " + attributes, attributes.isEmpty());
    }

    @Test
    public void testMessagePropertiesAreNotPartOfMappedAttributesUnlessMapped() {
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("source", "test-application");
        messageProperties.put("version", "1.2.3");
        messageProperties.put("environment", "production");
        givenMessageProperties(messageProperties);

        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        assertTrue("Unmapped message properties must not take part in the batch comparison but found " + attributes,
                attributes.isEmpty());
    }

    @Test
    public void testMessageKeyAndCustomMappingsStillWork() {
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("custom-prop", "custom-value");
        messageProperties.put("other-prop", "other-value");
        givenMessageProperties(messageProperties);
        when(mockMessage.getKey()).thenReturn("test-key");

        testRunner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "message.key=__KEY__,custom.attr=custom-prop");

        Map<String, String> attributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        assertEquals("Only the configured mappings take part in the batch comparison: " + attributes, 2, attributes.size());
        assertEquals("Message key should match", "test-key", attributes.get("message.key"));
        assertEquals("Custom property should match", "custom-value", attributes.get("custom.attr"));
        assertFalse(attributes.containsKey(MessageBatchAttributes.MESSAGE_ID_ATTRIBUTE));
        assertFalse(attributes.containsKey(MessageBatchAttributes.PROPERTY_ATTRIBUTE_PREFIX + "custom-prop"));
        assertFalse(attributes.containsKey(MessageBatchAttributes.PROPERTY_ATTRIBUTE_PREFIX + "other-prop"));
    }

    @Test
    public void testMappedAttributesOnlyDifferWhenMappedValuesDiffer() {
        testRunner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "tenant");

        Map<String, String> first = new HashMap<>();
        first.put("tenant", "A");
        first.put("trace-id", "trace-1");
        givenMessageProperties(first);
        when(mockMessageId.toString()).thenReturn("1:0:-1");
        Map<String, String> firstAttributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // same tenant, different message id and a different unmapped property: compatible with the first message
        Map<String, String> second = new HashMap<>();
        second.put("tenant", "A");
        second.put("trace-id", "trace-2");
        givenMessageProperties(second);
        when(mockMessageId.toString()).thenReturn("1:1:-1");
        Map<String, String> secondAttributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        // different tenant: must start a new FlowFile
        Map<String, String> third = new HashMap<>();
        third.put("tenant", "B");
        third.put("trace-id", "trace-2");
        givenMessageProperties(third);
        Map<String, String> thirdAttributes = processor.testGetMappedFlowFileAttributes(testRunner.getProcessContext(), mockMessage);

        assertEquals(firstAttributes, secondAttributes);
        assertNotEquals(secondAttributes, thirdAttributes);
        assertEquals("B", thirdAttributes.get("tenant"));
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
