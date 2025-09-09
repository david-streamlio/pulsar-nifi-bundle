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

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.utils.PublisherLease;
import org.apache.nifi.processors.pulsar.utils.PublisherPool;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class PublishPulsarLeaseReuseTest extends AbstractPulsarProcessorTest<byte[]> {

    @Mock
    private PublisherPool mockPublisherPool;

    @Mock
    private PublisherLease mockLease1;

    @Mock
    private PublisherLease mockLease2;

    private PublishPulsar processor;

    @Before
    public void setUp() throws InitializationException, Exception {
        MockitoAnnotations.openMocks(this);
        
        processor = new PublishPulsar();
        runner = TestRunners.newTestRunner(processor);
        addPulsarClientService();
        
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "test-topic");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        
        // Use reflection to inject the mock PublisherPool
        injectMockPublisherPool();
    }
    
    private void injectMockPublisherPool() throws Exception {
        // Get the private publisherPool field from AbstractPulsarProducerProcessor
        Field publisherPoolField = AbstractPulsarProducerProcessor.class.getDeclaredField("publisherPool");
        publisherPoolField.setAccessible(true);
        publisherPoolField.set(processor, mockPublisherPool);
    }
    
    private PublisherLease getCurrentLease() throws Exception {
        Field currentLeaseField = PublishPulsar.class.getDeclaredField("currentLease");
        currentLeaseField.setAccessible(true);
        return (PublisherLease) currentLeaseField.get(processor);
    }
    
    private PublisherLease callObtainPublisherLease(String topicName) throws Exception {
        Method obtainPublisherLeaseMethod = PublishPulsar.class.getDeclaredMethod("obtainPublisherLease", String.class);
        obtainPublisherLeaseMethod.setAccessible(true);
        return (PublisherLease) obtainPublisherLeaseMethod.invoke(processor, topicName);
    }

    @Test
    public void testLeaseReuseForSameTopic() throws Exception {
        // Setup mock lease to return consistent topic name
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn("topic1");
        
        // First call should create new lease
        PublisherLease lease1 = callObtainPublisherLease("topic1");
        assertSame("First call should return the mock lease", mockLease1, lease1);
        assertSame("Current lease should be set", mockLease1, getCurrentLease());
        
        // Second call with same topic should reuse existing lease
        PublisherLease lease2 = callObtainPublisherLease("topic1");
        assertSame("Second call should return the same lease", mockLease1, lease2);
        assertSame("Current lease should still be the same", mockLease1, getCurrentLease());
        
        // Verify pool was only called once
        verify(mockPublisherPool, times(1)).obtainPublisher("topic1");
        verify(mockLease1, never()).close();
    }

    @Test
    public void testLeaseRecreationForDifferentTopic() throws Exception {
        // Setup mock leases for different topics
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease1);
        when(mockPublisherPool.obtainPublisher("topic2")).thenReturn(mockLease2);
        when(mockLease1.getTopicName()).thenReturn("topic1");
        when(mockLease2.getTopicName()).thenReturn("topic2");
        
        // First call creates lease for topic1
        PublisherLease lease1 = callObtainPublisherLease("topic1");
        assertSame("First lease should be mockLease1", mockLease1, lease1);
        
        // Second call with different topic should close old lease and create new one
        PublisherLease lease2 = callObtainPublisherLease("topic2");
        assertSame("Second lease should be mockLease2", mockLease2, lease2);
        assertSame("Current lease should be updated", mockLease2, getCurrentLease());
        
        // Verify old lease was closed and new lease was created
        verify(mockLease1, times(1)).close();
        verify(mockPublisherPool, times(1)).obtainPublisher("topic1");
        verify(mockPublisherPool, times(1)).obtainPublisher("topic2");
    }

    @Test
    public void testNullTopicNameHandling() throws Exception {
        // Setup mock lease that returns null topic name
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn(null);
        
        // First call with real topic
        PublisherLease lease1 = callObtainPublisherLease("topic1");
        assertSame("First lease should be mockLease1", mockLease1, lease1);
        
        // Second call with same topic should not reuse because lease returns null topic
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease2);
        PublisherLease lease2 = callObtainPublisherLease("topic1");
        assertSame("Second lease should be mockLease2", mockLease2, lease2);
        
        // Verify old lease was closed because topic comparison failed
        verify(mockLease1, times(1)).close();
        verify(mockPublisherPool, times(2)).obtainPublisher("topic1");
    }

    @Test
    public void testRequestedTopicNameNullHandling() throws Exception {
        // Setup mock lease
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn("topic1");
        
        // First call with real topic
        PublisherLease lease1 = callObtainPublisherLease("topic1");
        assertSame("First lease should be mockLease1", mockLease1, lease1);
        
        // Second call with null topic should not reuse lease
        when(mockPublisherPool.obtainPublisher(null)).thenReturn(mockLease2);
        PublisherLease lease2 = callObtainPublisherLease(null);
        assertSame("Second lease should be mockLease2", mockLease2, lease2);
        
        // Verify old lease was closed
        verify(mockLease1, times(1)).close();
        verify(mockPublisherPool, times(1)).obtainPublisher("topic1");
        verify(mockPublisherPool, times(1)).obtainPublisher(null);
    }

    @Test
    public void testBothTopicNamesNullHandling() throws Exception {
        // Setup mock lease that returns null topic name
        when(mockPublisherPool.obtainPublisher(null)).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn(null);
        
        // First call with null topic
        PublisherLease lease1 = callObtainPublisherLease(null);
        assertSame("First lease should be mockLease1", mockLease1, lease1);
        
        // Second call with null topic should not reuse because both are null
        when(mockPublisherPool.obtainPublisher(null)).thenReturn(mockLease2);
        PublisherLease lease2 = callObtainPublisherLease(null);
        assertSame("Second lease should be mockLease2", mockLease2, lease2);
        
        // Verify old lease was closed
        verify(mockLease1, times(1)).close();
        verify(mockPublisherPool, times(2)).obtainPublisher(null);
    }

    @Test
    public void testPublisherPoolReturnsNull() throws Exception {
        // Setup mock pool to return null
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(null);
        
        // Call should return null and not crash
        PublisherLease lease = callObtainPublisherLease("topic1");
        assertNull("Lease should be null when pool returns null", lease);
        assertNull("Current lease should remain null", getCurrentLease());
        
        verify(mockPublisherPool, times(1)).obtainPublisher("topic1");
    }

    @Test
    public void testOnUnscheduledCleansUpLease() throws Exception {
        // Setup mock lease
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn("topic1");
        
        // Create a lease
        callObtainPublisherLease("topic1");
        assertSame("Current lease should be set", mockLease1, getCurrentLease());
        
        // Call onUnscheduled
        processor.onUnscheduled();
        
        // Verify lease was closed and cleared
        verify(mockLease1, times(1)).close();
        assertNull("Current lease should be null after onUnscheduled", getCurrentLease());
    }

    @Test
    public void testOnUnscheduledWithNullTopicName() throws Exception {
        // Setup mock lease that returns null topic name
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn(null);
        
        // Create a lease
        callObtainPublisherLease("topic1");
        assertSame("Current lease should be set", mockLease1, getCurrentLease());
        
        // Call onUnscheduled - should not crash despite null topic name
        processor.onUnscheduled();
        
        // Verify lease was closed and cleared
        verify(mockLease1, times(1)).close();
        assertNull("Current lease should be null after onUnscheduled", getCurrentLease());
    }

    @Test
    public void testOnUnscheduledWithNoCurrentLease() throws Exception {
        // Ensure no current lease
        assertNull("Current lease should be null initially", getCurrentLease());
        
        // Call onUnscheduled - should not crash
        processor.onUnscheduled();
        
        // Should still be null and no interactions with mocks
        assertNull("Current lease should still be null", getCurrentLease());
        verifyNoInteractions(mockLease1, mockLease2);
    }

    @Test
    public void testIntegrationWithFlowFileProcessing() throws Exception {
        // Setup mock lease
        when(mockPublisherPool.obtainPublisher("test-topic")).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn("test-topic");
        
        // Add a FlowFile
        runner.enqueue("test content".getBytes());
        
        // Run the processor
        runner.run(1);
        
        // Verify the lease was obtained and used
        verify(mockPublisherPool, times(1)).obtainPublisher("test-topic");
        verify(mockLease1, times(1)).publish(any(), any(), any(), any(), any(), anyBoolean());
        
        // Verify FlowFile was transferred successfully
        runner.assertAllFlowFilesTransferred(AbstractPulsarProducerProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(AbstractPulsarProducerProcessor.REL_SUCCESS);
        assertEquals("Should have one success FlowFile", 1, successFlowFiles.size());
    }

    @Test
    public void testIntegrationWithMultipleFlowFilesSameTopic() throws Exception {
        // Setup mock lease
        when(mockPublisherPool.obtainPublisher("test-topic")).thenReturn(mockLease1);
        when(mockLease1.getTopicName()).thenReturn("test-topic");
        
        // Add multiple FlowFiles
        runner.enqueue("content1".getBytes());
        runner.enqueue("content2".getBytes());
        runner.enqueue("content3".getBytes());
        
        // Run the processor
        runner.run(1);
        
        // Verify the lease was obtained only once and reused
        verify(mockPublisherPool, times(1)).obtainPublisher("test-topic");
        verify(mockLease1, times(3)).publish(any(), any(), any(), any(), any(), anyBoolean());
        verify(mockLease1, never()).close(); // Should not be closed during processing
        
        // Verify all FlowFiles were transferred successfully
        runner.assertAllFlowFilesTransferred(AbstractPulsarProducerProcessor.REL_SUCCESS, 3);
    }

    @Test
    public void testIntegrationWithMultipleFlowFilesDifferentTopics() throws Exception {
        // Use expression language to vary topic names
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "${topic.name}");
        
        // Setup mock leases
        when(mockPublisherPool.obtainPublisher("topic1")).thenReturn(mockLease1);
        when(mockPublisherPool.obtainPublisher("topic2")).thenReturn(mockLease2);
        when(mockLease1.getTopicName()).thenReturn("topic1");
        when(mockLease2.getTopicName()).thenReturn("topic2");
        
        // Add FlowFiles with different topic attributes
        runner.enqueue("content1".getBytes(), java.util.Collections.singletonMap("topic.name", "topic1"));
        runner.enqueue("content2".getBytes(), java.util.Collections.singletonMap("topic.name", "topic2"));
        runner.enqueue("content3".getBytes(), java.util.Collections.singletonMap("topic.name", "topic1"));
        
        // Run the processor
        runner.run(1);
        
        // Verify leases were obtained for both topics
        verify(mockPublisherPool, times(2)).obtainPublisher("topic1"); // Called twice due to topic switch
        verify(mockPublisherPool, times(1)).obtainPublisher("topic2");
        
        // Verify lease closure when switching topics
        verify(mockLease1, times(1)).close(); // Closed when switching to topic2
        verify(mockLease2, times(1)).close(); // Closed when switching back to topic1
        
        // Verify all FlowFiles were processed
        runner.assertAllFlowFilesTransferred(AbstractPulsarProducerProcessor.REL_SUCCESS, 3);
    }
}