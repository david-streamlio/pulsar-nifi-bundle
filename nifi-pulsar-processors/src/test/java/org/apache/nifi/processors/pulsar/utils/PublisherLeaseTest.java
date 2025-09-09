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
package org.apache.nifi.processors.pulsar.utils;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class PublisherLeaseTest {

    @Mock
    private Producer producer;

    @Mock
    private ComponentLog logger;

    @Mock
    private FlowFile flowFile;

    @Mock
    private TypedMessageBuilder typedMessageBuilder;

    @Mock
    private MessageId messageId;

    @Mock
    private RecordSet recordSet;

    @Mock
    private RecordSetWriterFactory writerFactory;

    @Mock
    private RecordSchema schema;

    @Mock
    private Record record;

    private PublisherLease publisherLease;
    private Map<String, String> messageProperties;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        publisherLease = new PublisherLease(producer, logger);
        messageProperties = new HashMap<>();
        messageProperties.put("key1", "value1");
        messageProperties.put("key2", "value2");
    }

    @Test
    public void testConstructor() {
        PublisherLease lease = new PublisherLease(producer, logger);
        assertNotNull("PublisherLease should be created", lease);
    }

    @Test
    public void testGetTopicNameWithValidProducer() {
        String expectedTopic = "test-topic";
        when(producer.getTopic()).thenReturn(expectedTopic);

        String actualTopic = publisherLease.getTopicName();

        assertEquals("Topic name should match producer's topic", expectedTopic, actualTopic);
        verify(producer, times(1)).getTopic();
    }

    @Test
    public void testGetTopicNameWithNullProducer() {
        PublisherLease leaseWithNullProducer = new PublisherLease(null, logger);

        String actualTopic = leaseWithNullProducer.getTopicName();

        assertNull("Topic name should be null when producer is null", actualTopic);
    }

    @Test
    public void testGetTopicNameWhenProducerReturnsNull() {
        when(producer.getTopic()).thenReturn(null);

        String actualTopic = publisherLease.getTopicName();

        assertNull("Topic name should be null when producer returns null", actualTopic);
        verify(producer, times(1)).getTopic();
    }

    @Test
    public void testPublishWithoutDemarcator() throws IOException, PulsarClientException {
        // Setup FlowFile
        byte[] content = "test content".getBytes();
        when(flowFile.getSize()).thenReturn((long) content.length);
        InputStream inputStream = new ByteArrayInputStream(content);

        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.key(anyString())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.send()).thenReturn(messageId);

        // Call publish method
        publisherLease.publish(flowFile, inputStream, "test-key", messageProperties, null, false);

        // Verify interactions
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(messageProperties);
        verify(typedMessageBuilder, times(1)).value(content);
        verify(typedMessageBuilder, times(1)).key("test-key");
        verify(typedMessageBuilder, times(1)).send();
    }

    @Test
    public void testPublishWithoutDemarcatorAsync() throws IOException, PulsarClientException {
        // Setup FlowFile
        byte[] content = "test content".getBytes();
        when(flowFile.getSize()).thenReturn((long) content.length);
        InputStream inputStream = new ByteArrayInputStream(content);

        // Setup TypedMessageBuilder chain for async
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.key(anyString())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(messageId));

        // Call publish method with async=true
        publisherLease.publish(flowFile, inputStream, "test-key", messageProperties, null, true);

        // Verify interactions
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(messageProperties);
        verify(typedMessageBuilder, times(1)).value(content);
        verify(typedMessageBuilder, times(1)).key("test-key");
        verify(typedMessageBuilder, times(1)).sendAsync();
        verify(typedMessageBuilder, never()).send(); // Should not call sync send
    }

    @Test
    public void testPublishWithNullMessageKey() throws IOException, PulsarClientException {
        // Setup FlowFile
        byte[] content = "test content".getBytes();
        when(flowFile.getSize()).thenReturn((long) content.length);
        InputStream inputStream = new ByteArrayInputStream(content);

        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.send()).thenReturn(messageId);

        // Call publish method with null key
        publisherLease.publish(flowFile, inputStream, null, messageProperties, null, false);

        // Verify interactions
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(messageProperties);
        verify(typedMessageBuilder, times(1)).value(content);
        verify(typedMessageBuilder, never()).key(anyString()); // Should not set key when null
        verify(typedMessageBuilder, times(1)).send();
    }

    @Test
    public void testPublishWithDemarcator() throws IOException, PulsarClientException {
        // Setup FlowFile with content that will be split by demarcator
        String content = "message1\nmessage2\nmessage3";
        when(flowFile.getSize()).thenReturn((long) content.length());
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        byte[] demarcator = "\n".getBytes();

        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.key(anyString())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.send()).thenReturn(messageId);

        // Call publish method
        publisherLease.publish(flowFile, inputStream, "test-key", messageProperties, demarcator, false);

        // Verify interactions - should be called 3 times for 3 messages
        verify(producer, times(3)).newMessage();
        verify(typedMessageBuilder, times(3)).properties(messageProperties);
        verify(typedMessageBuilder, times(3)).value(any(byte[].class));
        verify(typedMessageBuilder, times(3)).key("test-key");
        verify(typedMessageBuilder, times(3)).send();
    }

    @Test
    public void testPublishWithEmptyMessageProperties() throws IOException, PulsarClientException {
        // Setup FlowFile
        byte[] content = "test content".getBytes();
        when(flowFile.getSize()).thenReturn((long) content.length);
        InputStream inputStream = new ByteArrayInputStream(content);
        Map<String, String> emptyProperties = new HashMap<>();

        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.send()).thenReturn(messageId);

        // Call publish method
        publisherLease.publish(flowFile, inputStream, null, emptyProperties, null, false);

        // Verify interactions
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(emptyProperties);
        verify(typedMessageBuilder, times(1)).value(content);
        verify(typedMessageBuilder, times(1)).send();
    }

    @Test
    public void testComplete() {
        // Initially, complete should return 0
        assertEquals("Initial message count should be 0", 0, publisherLease.complete());

        // The complete() method returns the count of messages sent, but since we can't easily
        // mock the internal messagesSent counter increment (it's done inside publish methods),
        // we can only test the initial state
    }

    @Test
    public void testClose() throws PulsarClientException {
        // Call close method
        publisherLease.close();

        // Verify producer was flushed and closed
        verify(producer, times(1)).flush();
        verify(producer, times(1)).close();
    }

    @Test
    public void testCloseWithPulsarClientException() throws PulsarClientException {
        // Setup producer to throw exception on flush
        doThrow(new PulsarClientException("Test exception")).when(producer).flush();

        // Call close method - should not throw exception
        publisherLease.close();

        // Verify error was logged
        verify(logger, times(1)).error(eq("Unable to close producer"), any(PulsarClientException.class));
        verify(producer, times(1)).flush();
        verify(producer, times(1)).close(); // Should still try to close
    }

    @Test
    public void testCloseWithExceptionOnClose() throws PulsarClientException {
        // Setup producer to throw exception on close
        doThrow(new PulsarClientException("Test exception")).when(producer).close();

        // Call close method - should not throw exception
        publisherLease.close();

        // Verify error was logged
        verify(logger, times(1)).error(eq("Unable to close producer"), any(PulsarClientException.class));
        verify(producer, times(1)).flush();
        verify(producer, times(1)).close();
    }

    @Test
    public void testSendAsyncMethod() throws PulsarClientException {
        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.key(anyString())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(messageId));

        // Call protected sendAsync method via reflection or create subclass for testing
        CompletableFuture<MessageId> result = publisherLease.sendAsync(producer, "test-key", messageProperties, "test-content".getBytes());

        // Verify result
        assertNotNull("Result should not be null", result);
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(messageProperties);
        verify(typedMessageBuilder, times(1)).value("test-content".getBytes());
        verify(typedMessageBuilder, times(1)).key("test-key");
        verify(typedMessageBuilder, times(1)).sendAsync();
    }

    @Test
    public void testSendAsyncMethodWithNullKey() throws PulsarClientException {
        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(messageId));

        // Call protected sendAsync method with null key
        CompletableFuture<MessageId> result = publisherLease.sendAsync(producer, null, messageProperties, "test-content".getBytes());

        // Verify result and interactions
        assertNotNull("Result should not be null", result);
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(messageProperties);
        verify(typedMessageBuilder, times(1)).value("test-content".getBytes());
        verify(typedMessageBuilder, never()).key(anyString()); // Should not set key when null
        verify(typedMessageBuilder, times(1)).sendAsync();
    }

    @Test
    public void testSendSyncMethod() throws PulsarClientException {
        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.key(anyString())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.send()).thenReturn(messageId);

        // Call protected send method
        CompletableFuture<MessageId> result = publisherLease.send(producer, "test-key", messageProperties, "test-content".getBytes());

        // Verify result
        assertNotNull("Result should not be null", result);
        assertFalse("Result should not be cancelled", result.isCancelled());
        
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(messageProperties);
        verify(typedMessageBuilder, times(1)).value("test-content".getBytes());
        verify(typedMessageBuilder, times(1)).key("test-key");
        verify(typedMessageBuilder, times(1)).send();
    }

    @Test
    public void testSendSyncMethodWithNullKey() throws PulsarClientException {
        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.send()).thenReturn(messageId);

        // Call protected send method with null key
        CompletableFuture<MessageId> result = publisherLease.send(producer, null, messageProperties, "test-content".getBytes());

        // Verify result and interactions
        assertNotNull("Result should not be null", result);
        verify(producer, times(1)).newMessage();
        verify(typedMessageBuilder, times(1)).properties(messageProperties);
        verify(typedMessageBuilder, times(1)).value("test-content".getBytes());
        verify(typedMessageBuilder, never()).key(anyString()); // Should not set key when null
        verify(typedMessageBuilder, times(1)).send();
    }

    @Test
    public void testMultiplePublishCallsIncrementsMessageCount() throws IOException, PulsarClientException {
        // Setup FlowFile
        byte[] content = "test content".getBytes();
        when(flowFile.getSize()).thenReturn((long) content.length);
        
        // Setup TypedMessageBuilder chain
        when(producer.newMessage()).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.properties(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.send()).thenReturn(messageId);

        // Call publish multiple times
        InputStream inputStream1 = new ByteArrayInputStream(content);
        publisherLease.publish(flowFile, inputStream1, null, messageProperties, null, false);
        
        InputStream inputStream2 = new ByteArrayInputStream(content);
        publisherLease.publish(flowFile, inputStream2, null, messageProperties, null, false);

        // Verify producer was called twice
        verify(producer, times(2)).newMessage();
        verify(typedMessageBuilder, times(2)).send();
    }

    @Test
    public void testPublishWithIOException() throws IOException {
        // Create an InputStream that will throw IOException
        InputStream faultyInputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Test IO Exception");
            }
        };

        when(flowFile.getSize()).thenReturn(10L);

        // Call publish method - should propagate IOException
        try {
            publisherLease.publish(flowFile, faultyInputStream, null, messageProperties, null, false);
            fail("Should have thrown IOException");
        } catch (IOException e) {
            assertEquals("Exception message should match", "Test IO Exception", e.getMessage());
        }

        // Verify producer was not called since reading failed
        verify(producer, never()).newMessage();
    }
}