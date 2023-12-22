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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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
    protected Message<GenericRecord>[] mockMessages = new Message[0];

    @Mock
    SchemaInfo mockSchema = mock(SchemaInfo.class);

    @Mock
    MessageId mockMessageId = mock(MessageId.class);

    CompletableFuture<MessageId> future;

    public MockPulsarClientService() {
        when(mockClient.newProducer()).thenReturn((ProducerBuilder<byte[]>) mockProducerBuilder);
        when(mockClient.newConsumer(Schema.AUTO_CONSUME())).thenReturn((ConsumerBuilder<GenericRecord>) mockConsumerBuilder);
        when(mockClient.newConsumer(any(Schema.class))).thenReturn((ConsumerBuilder<GenericRecord>) mockConsumerBuilder);

        when(mockProducerBuilder.loadConf(any(Map.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.topic(anyString())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.enableBatching(anyBoolean())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.batchingMaxBytes(anyInt())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.batchingMaxMessages(anyInt())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.batchingMaxPublishDelay(anyLong(), any(TimeUnit.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.blockIfQueueFull(anyBoolean())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.compressionType(any(CompressionType.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.maxPendingMessages(anyInt())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.messageRoutingMode(any(MessageRoutingMode.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.autoUpdatePartitions(anyBoolean())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.autoUpdatePartitionsInterval(anyInt(), any(TimeUnit.class))).thenReturn(mockProducerBuilder);

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
        when(mockConsumerBuilder.replicateSubscriptionState(anyBoolean())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.autoUpdatePartitions(anyBoolean())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.autoUpdatePartitionsInterval(anyInt(), any(TimeUnit.class))).thenReturn(mockConsumerBuilder);

        when(mockSchema.getType()).thenReturn(SchemaType.BYTES);

        try {
            when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);
            when(mockConsumer.isConnected()).thenReturn(true);

            if (mockMessages.length >1 ) {
                setMockMessages(Arrays.asList(mockMessages));
            } else if (mockMessages.length==1){
                setMockMessage(mockMessages[0]);
            }else{
                setMockMessage(null);
            }


            when(mockProducerBuilder.create()).thenReturn(mockProducer);
            defineDefaultProducerBehavior();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * sets a message and this message will be returned infinite amount of times by the consumer
     */
    public void setMockMessage(Message<GenericRecord> mockMessage2) {
        this.mockMessages = new Message[]{mockMessage2};

        // Configure the consumer behavior
        try {
            when(mockConsumer.receive()).thenReturn(mockMessage2);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        CompletableFuture<Message<GenericRecord>> future = CompletableFuture.supplyAsync(() -> {
            return mockMessage2;
        });

        when(mockConsumer.receiveAsync()).thenReturn(future);

        try {
            doAnswer(new Answer<Message<GenericRecord>>() {
                public Message<GenericRecord> answer(InvocationOnMock invocation) {
                    return mockMessage2;
                }
            }).when(mockConsumer).receive(0, TimeUnit.SECONDS);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public void setMockMessages(List<Message<GenericRecord>> mockMessages2) {
        this.mockMessages = mockMessages2.toArray(new Message[]{});

        // Configure the consumer behavior
        try {
            when(mockConsumer.receive()).thenReturn(mockMessages[0], Arrays.copyOfRange(mockMessages, 1, mockMessages.length));
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        List<CompletableFuture<Message<GenericRecord>>> future = Arrays.stream(mockMessages)
                .map(mockMessage -> CompletableFuture.supplyAsync(() -> mockMessage))
                .collect(Collectors.toList());

        when(mockConsumer.receiveAsync()).thenReturn(future.get(0), (CompletableFuture<Message<GenericRecord>>[]) future.subList(1, future.size()).toArray(new CompletableFuture<?>[]{}));

        try {
            when(mockConsumer.receive(0, TimeUnit.SECONDS)).thenReturn(mockMessages[0], Arrays.copyOfRange(mockMessages, 1, mockMessages.length));
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
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
