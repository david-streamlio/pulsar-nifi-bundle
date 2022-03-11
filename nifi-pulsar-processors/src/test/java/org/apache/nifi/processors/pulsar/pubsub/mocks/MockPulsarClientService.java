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
package org.apache.nifi.processors.pulsar.pubsub.mocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;

@SuppressWarnings("unchecked")
public class MockPulsarClientService<T> extends AbstractControllerService implements PulsarClientService {

    @Mock
    PulsarClient mockClient = mock(PulsarClient.class);
    
    @Mock
    PulsarAdmin mockAdmin = mock(PulsarAdmin.class);

	@Mock
    ProducerBuilder<T> mockProducerBuilder = mock(ProducerBuilder.class);

    @Mock
    ConsumerBuilder<GenericRecord> mockConsumerBuilder = mock(ConsumerBuilder.class);

    @Mock
    Producer<T> mockProducer = mock(Producer.class);

    @Mock
    Consumer<GenericRecord> mockConsumer = mock(Consumer.class);

    @Mock
    TypedMessageBuilder<T> mockTypedMessageBuilder = mock(TypedMessageBuilder.class);

    @Mock
    protected Message<GenericRecord> mockMessage;
    
    @Mock
    SchemaInfo mockSchema = mock(SchemaInfo.class);

    @Mock
    MessageId mockMessageId = mock(MessageId.class);

    CompletableFuture<MessageId> future;

    public MockPulsarClientService() {
        when(mockClient.newProducer()).thenReturn((ProducerBuilder<byte[]>) mockProducerBuilder);
        when(mockClient.newConsumer(Schema.AUTO_CONSUME())).thenReturn((ConsumerBuilder<GenericRecord>) mockConsumerBuilder);
        when(mockClient.newConsumer(any(Schema.class))).thenReturn((ConsumerBuilder<GenericRecord>) mockConsumerBuilder);
        
        when(mockProducerBuilder.topic(anyString())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.enableBatching(anyBoolean())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.batchingMaxMessages(anyInt())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.batchingMaxPublishDelay(anyLong(), any(TimeUnit.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.blockIfQueueFull(anyBoolean())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.compressionType(any(CompressionType.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.maxPendingMessages(anyInt())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.messageRoutingMode(any(MessageRoutingMode.class))).thenReturn(mockProducerBuilder);

        when(mockConsumerBuilder.topic(any(String[].class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.topic(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.ackTimeout(anyLong(), any(TimeUnit.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.consumerName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.cryptoFailureAction(any(ConsumerCryptoFailureAction.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.priorityLevel(anyInt())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.receiverQueueSize(anyInt())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionInitialPosition(any(SubscriptionInitialPosition.class))).thenReturn(mockConsumerBuilder);

        when(mockSchema.getType()).thenReturn(SchemaType.BYTES);

        try {
            when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);
            when(mockConsumer.isConnected()).thenReturn(true);
            when(mockConsumer.receive()).thenReturn(mockMessage);
            doAnswer(new Answer<Message<GenericRecord>>() {
               public Message<GenericRecord> answer(InvocationOnMock invocation) {
                       return mockMessage;
               }
             }).when(mockConsumer).receive(0, TimeUnit.SECONDS);

            when(mockProducerBuilder.create()).thenReturn(mockProducer);
            defineDefaultProducerBehavior();
        } catch (PulsarClientException e) {
           e.printStackTrace();
        }
    }

    public void setMockMessage(Message<GenericRecord> mockMessage2) {
        this.mockMessage = mockMessage2;

        // Configure the consumer behavior
        try {
          when(mockConsumer.receive()).thenReturn(mockMessage);
        } catch (PulsarClientException e) {
          e.printStackTrace();
        }

        CompletableFuture<Message<GenericRecord>> future = CompletableFuture.supplyAsync(() -> {
           return mockMessage;
        });

        when(mockConsumer.receiveAsync()).thenReturn(future);
    }

    public Producer<T> getMockProducer() {
        return mockProducer;
    }

    public void setMockProducer(Producer<T> mockProducer) {
       this.mockProducer = mockProducer;
       defineDefaultProducerBehavior();
    }

    private void defineDefaultProducerBehavior() {
        try {
            // when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<T>() {
            //    @Override
            //    public boolean matches(Object argument) {
            //        return true;
            //    }
            // }))).thenReturn(mockMessageId);

            future = CompletableFuture.supplyAsync(() -> {
                return mock(MessageId.class);
            });

            // when(mockProducer.sendAsync(Matchers.argThat(new ArgumentMatcher<T>() {
            //    @Override
            //    public boolean matches(Object argument) {
            //        return true;
            //    }
            // }))).thenReturn(future);

            when(mockProducer.isConnected()).thenReturn(true);
            when(mockProducer.newMessage()).thenReturn(mockTypedMessageBuilder);
            when(mockTypedMessageBuilder.key(anyString())).thenReturn(mockTypedMessageBuilder);
            when(mockTypedMessageBuilder.properties((Map<String, String>) any(Map.class))).thenReturn(mockTypedMessageBuilder);
            when(mockTypedMessageBuilder.value((T) any(byte[].class))).thenReturn(mockTypedMessageBuilder);
            when(mockTypedMessageBuilder.send()).thenReturn(mockMessageId);
            when(mockTypedMessageBuilder.sendAsync()).thenReturn(future);

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public Consumer<GenericRecord> getMockConsumer() {
      return mockConsumer;
    }

    public ProducerBuilder<T> getMockProducerBuilder() {
      return mockProducerBuilder;
    }

    public ConsumerBuilder<GenericRecord> getMockConsumerBuilder() {
      return mockConsumerBuilder;
    }

    public TypedMessageBuilder<T> getMockTypedMessageBuilder() {
      return mockTypedMessageBuilder;
    }

    @Override
    public PulsarClient getPulsarClient() {
      return mockClient;
    }

    @Override
    public String getPulsarBrokerRootURL() {
       return "pulsar://mocked:6650";
    }

}
