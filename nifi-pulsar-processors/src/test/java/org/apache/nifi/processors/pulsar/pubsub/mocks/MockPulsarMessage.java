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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.EncryptionContext;

import java.util.Map;
import java.util.Optional;

public class MockPulsarMessage<T> implements Message<T> {
    
    private final String topic;
    private final byte[] data;
    private final MessageId messageId;
    private final Map<String, String> properties;
    private final String key;
    private final T value;
    
    public MockPulsarMessage(String topic, byte[] data, String messageIdStr, Map<String, String> properties, String key) {
        this.topic = topic;
        this.data = data;
        this.messageId = messageIdStr != null ? createMessageId(messageIdStr) : null;
        this.properties = properties;
        this.key = key;
        this.value = null;
    }
    
    public MockPulsarMessage(String topic, T value, String messageIdStr, Map<String, String> properties, String key) {
        this.topic = topic;
        this.data = null;
        this.messageId = messageIdStr != null ? createMessageId(messageIdStr) : null;
        this.properties = properties;
        this.key = key;
        this.value = value;
    }
    
    private MessageId createMessageId(String messageIdStr) {
        // Create a simple mock MessageId that returns the string representation
        return new MessageId() {
            @Override
            public int compareTo(MessageId o) {
                return messageIdStr.compareTo(o.toString());
            }

            @Override
            public byte[] toByteArray() {
                return messageIdStr.getBytes();
            }
            
            @Override
            public String toString() {
                return messageIdStr;
            }
        };
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean hasProperty(String name) {
        return properties != null && properties.containsKey(name);
    }

    @Override
    public String getProperty(String name) {
        return properties != null ? properties.get(name) : null;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public long getPublishTime() {
        return System.currentTimeMillis();
    }

    @Override
    public long getEventTime() {
        return 0;
    }

    @Override
    public long getSequenceId() {
        return 0;
    }

    @Override
    public String getProducerName() {
        return "mock-producer";
    }

    @Override
    public boolean hasKey() {
        return key != null;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public boolean hasBase64EncodedKey() {
        return false;
    }

    @Override
    public byte[] getKeyBytes() {
        return key != null ? key.getBytes() : null;
    }

    @Override
    public boolean hasOrderingKey() {
        return false;
    }

    @Override
    public byte[] getOrderingKey() {
        return null;
    }

    @Override
    public String getTopicName() {
        return topic;
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        return Optional.empty();
    }

    @Override
    public int getRedeliveryCount() {
        return 0;
    }

    @Override
    public byte[] getSchemaVersion() {
        return null;
    }

    @Override
    public boolean isReplicated() {
        return false;
    }

    @Override
    public String getReplicatedFrom() {
        return null;
    }

    @Override
    public void release() {
        // No-op
    }

    @Override
    public boolean hasBrokerPublishTime() {
        return false;
    }

    @Override
    public Optional<Long> getBrokerPublishTime() {
        return Optional.empty();
    }

    @Override
    public boolean hasIndex() {
        return false;
    }

    @Override
    public Optional<Long> getIndex() {
        return Optional.empty();
    }

    // Additional methods that might be needed for schema handling
    @Override
    public Optional<Schema<?>> getReaderSchema() {
        return Optional.empty();
    }
}