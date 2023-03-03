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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestConsumePulsar extends AbstractPulsarProcessorTest<byte[]> {

    @Mock
    protected Message<GenericRecord> mockMessage;

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @SuppressWarnings("unchecked")
	@Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        mockMessage = mock(Message.class);
        addPulsarClientService();
    }

    @Test
    public void singleSyncMessageTest() throws PulsarClientException {
        this.sendMessages("Mocked Message", "foo", "bar", false, 1);
    }

    @Test
    public void multipleSeparateSyncMessagesTest() throws PulsarClientException {
    	this.sendMessages("Mocked Message", "foo", "bar", false, 10);
    }
    
    @Test
    public void multipleSyncMessagesBatchTest() throws PulsarClientException {
    	this.batchMessages("Mocked Message", "foo", "bar", false, 10);
    }
    
    @Test
    public void multipleSyncMessagesBatchSharedSubTest() throws PulsarClientException {
    	this.batchMessages("Mocked Message", "foo", "bar", false, 10, "Shared");
    }
    
    @Test
    public void singleAsyncMessageTest() throws PulsarClientException {
        this.sendMessages("Mocked Message", "foo", "bar", true, 1);
    }

    @Test
    public void multipleSeparateAsyncMessagesTest() throws PulsarClientException {
        this.sendMessages("Mocked Message", "foo", "bar", true, 10);
    }

    @Test
    public void multipleAsyncMessagesBatchTest() throws PulsarClientException {
    	this.batchMessages("Mocked Message", "foo", "bar", true, 10);
    }
    
    @Test
    public void multipleAsyncMessagesBatchSharedSubTest() throws PulsarClientException {
    	this.batchMessages("Mocked Message", "foo", "bar", true, 10, "Shared");
    }

    @Test
    public void keySharedTest() throws PulsarClientException {
    	when(mockMessage.getData()).thenReturn("Mocked Message".getBytes());
    	mockClientService.setMockMessage(mockMessage);
    	
    	runner.setProperty(ConsumePulsar.TOPICS, "foo");
    	runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
    	runner.setProperty(ConsumePulsar.SUBSCRIPTION_TYPE, "Key_Shared");
    	runner.run(10, true);
    	runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);
    	
    	runner.assertQueueEmpty();
    	
    	verify(mockClientService.getMockConsumerBuilder(), times(1)).subscriptionType(SubscriptionType.Key_Shared);
    }
    
    protected void batchMessages(String msg, String topic, String sub, boolean async, int batchSize) throws PulsarClientException {
    	batchMessages(msg, topic, sub, async, batchSize, "Exclusive");
    }
    
    protected void batchMessages(String msg, String topic, String sub, boolean async, int batchSize, String subType) throws PulsarClientException {
        when(mockMessage.getData()).thenReturn(msg.getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsar.TOPICS, topic);
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, sub);
        runner.setProperty(ConsumePulsar.CONSUMER_BATCH_SIZE, batchSize + "");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_TYPE, subType);
        runner.setProperty(ConsumePulsar.MESSAGE_DEMARCATOR, "\n");
        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        flowFiles.get(0).assertAttributeEquals(ConsumePulsar.MSG_COUNT, batchSize + "");

        StringBuffer sb = new StringBuffer();
        
        // expect demarcators to occur between messages
        for (int idx = 0; idx < batchSize - 1; idx++) {
            sb.append(msg);
            sb.append('\n');
        }

        sb.append(msg);
        
        flowFiles.get(0).assertContentEquals(sb.toString());

        boolean shared = isSharedSubType(subType);
        
        // Verify that every message was acknowledged
        verify(mockClientService.getMockConsumer(), times(batchSize)).receive(0, TimeUnit.SECONDS);
        
        if (shared) {
        	if (async) {
        		verify(mockClientService.getMockConsumer(), times(batchSize)).acknowledgeAsync(mockMessage);
        	} else {
        		verify(mockClientService.getMockConsumer(), times(batchSize)).acknowledge(mockMessage);
        	}
        } else {
        	if (async) {
                verify(mockClientService.getMockConsumer(), times(1)).acknowledgeCumulativeAsync(mockMessage);        		
        	} else {
                verify(mockClientService.getMockConsumer(), times(1)).acknowledgeCumulative(mockMessage);        		
        	}
        }
    }

    protected void sendMessages(String msg, String topic, String sub, boolean async, int iterations) throws PulsarClientException {
    	sendMessages(msg, topic, sub, async, iterations, "Exclusive");
    }
    
    protected void sendMessages(String msg, String topic, String sub, boolean async, int iterations, String subType) throws PulsarClientException {

        when(mockMessage.getData()).thenReturn(msg.getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsar.TOPICS, topic);
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, sub);
        runner.setProperty(ConsumePulsar.CONSUMER_BATCH_SIZE, 1 + "");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_TYPE, subType);
        runner.run(iterations, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(iterations, flowFiles.size());

        for (MockFlowFile ff : flowFiles) {
            ff.assertContentEquals(msg);        
        }

        verify(mockClientService.getMockConsumer(), times(iterations)).receive(0, TimeUnit.SECONDS);

        boolean shared = isSharedSubType(subType);
        
        // Verify that every message was acknowledged
        if (shared) {
        	if (async) {
        		verify(mockClientService.getMockConsumer(), times(iterations)).acknowledgeAsync(mockMessage);
        	} else {
        		verify (mockClientService.getMockConsumer(), times(iterations)).acknowledge(mockMessage);
        	}
        } else {
        	if (async) {
        		verify(mockClientService.getMockConsumer(), times(iterations)).acknowledgeCumulativeAsync(mockMessage);
        	} else {
        		verify(mockClientService.getMockConsumer(), times(iterations)).acknowledgeCumulative(mockMessage);
        	}
        }        
    }

    protected void doMappedAttributesTest() throws PulsarClientException {
        when(mockMessage.getData())
          .thenReturn("A".getBytes())
          .thenReturn("B".getBytes())
          .thenReturn("C".getBytes())
          .thenReturn("D".getBytes());
        
        when(mockMessage.getProperty("prop"))
          .thenReturn(null)
          .thenReturn(null)
          .thenReturn("val")
          .thenReturn("val");
        
        when(mockMessage.getKey())
          .thenReturn(null)
          .thenReturn(null)
          .thenReturn(null)
          .thenReturn("K");
        
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.MAPPED_FLOWFILE_ATTRIBUTES, "prop,key=__KEY__");
        runner.setProperty(ConsumePulsar.CONSUMER_BATCH_SIZE, "4");
        runner.setProperty(ConsumePulsar.MESSAGE_DEMARCATOR, "===");
        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_TYPE, "Exclusive");

        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);
        runner.assertQueueEmpty();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(3, flowFiles.size());

        // first flow file should have A, second should have B
        flowFiles.get(0).assertAttributeNotExists("prop");
        flowFiles.get(0).assertAttributeNotExists("key");
        flowFiles.get(0).assertContentEquals("A===B");
        flowFiles.get(1).assertAttributeEquals("prop", "val");
        flowFiles.get(1).assertAttributeNotExists("key");
        flowFiles.get(1).assertContentEquals("C");
        flowFiles.get(2).assertAttributeEquals("prop", "val");
        flowFiles.get(2).assertAttributeEquals("key", "K");
        flowFiles.get(2).assertContentEquals("D");
    }
}
