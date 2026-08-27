/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

/**
 * Accumulates the Pulsar metadata (message id, message properties) of the messages written into a
 * single FlowFile and derives the FlowFile attributes that describe the whole batch.
 *
 * <p>This metadata is informational only. It is deliberately kept out of the comparison that decides
 * whether a message may be appended to the FlowFile currently being written (the user-configured
 * "Mapped FlowFile Attributes", see {@code AbstractPulsarConsumerProcessor#getMappedFlowFileAttributes}):
 * a value such as the message id changes with every message, so including it in that comparison would
 * close the FlowFile after each message and defeat "Consumer Message Batch Size".</p>
 *
 * <p>Attributes produced by {@link #toAttributes()}:</p>
 * <ul>
 *   <li>{@value #MESSAGE_ID_ATTRIBUTE}: set only when the FlowFile holds exactly one message (unchanged
 *       behaviour for single-message FlowFiles). Omitted otherwise, because no single id describes a
 *       batch of messages.</li>
 *   <li>{@value #FIRST_MESSAGE_ID_ATTRIBUTE} / {@value #LAST_MESSAGE_ID_ATTRIBUTE}: ids of the first and
 *       of the last message in the FlowFile, always set when the ids are available. The full list of ids
 *       is intentionally never materialised as an attribute.</li>
 *   <li>{@value #PROPERTY_ATTRIBUTE_PREFIX}{@code <name>}: every message property whose value is identical
 *       for all messages in the FlowFile ("keep only common attributes", the same rule NiFi's MergeContent
 *       applies). A single-message FlowFile therefore carries all of its properties. To force messages
 *       with different values of a property into different FlowFiles, map that property through
 *       "Mapped FlowFile Attributes".</li>
 * </ul>
 */
public final class MessageBatchAttributes {

    public static final String MESSAGE_ID_ATTRIBUTE = "pulsar.message.id";
    public static final String FIRST_MESSAGE_ID_ATTRIBUTE = "pulsar.message.id.first";
    public static final String LAST_MESSAGE_ID_ATTRIBUTE = "pulsar.message.id.last";
    public static final String PROPERTY_ATTRIBUTE_PREFIX = "pulsar.property.";

    private int messageCount;
    private MessageId firstMessageId;
    private MessageId lastMessageId;
    private Map<String, String> commonProperties;

    /**
     * Records the metadata of a message that has been written into the FlowFile.
     *
     * @param message the message just appended to the FlowFile
     */
    public void add(final Message<?> message) {
        final MessageId messageId = message.getMessageId();
        final Map<String, String> properties = message.getProperties();

        if (messageCount == 0) {
            firstMessageId = messageId;
            commonProperties = properties == null ? new HashMap<>() : new HashMap<>(properties);
        } else {
            // keep only the properties that carry the same value in every message of the batch
            commonProperties.entrySet().removeIf(entry ->
                    properties == null || !Objects.equals(entry.getValue(), properties.get(entry.getKey())));
        }

        lastMessageId = messageId;
        messageCount++;
    }

    /**
     * @return the number of messages recorded so far
     */
    public int getMessageCount() {
        return messageCount;
    }

    /**
     * @return the FlowFile attributes describing the messages recorded so far (empty if none)
     */
    public Map<String, String> toAttributes() {
        final Map<String, String> attributes = new HashMap<>();

        if (messageCount == 0) {
            return attributes;
        }

        if (firstMessageId != null) {
            if (messageCount == 1) {
                attributes.put(MESSAGE_ID_ATTRIBUTE, firstMessageId.toString());
            }
            attributes.put(FIRST_MESSAGE_ID_ATTRIBUTE, firstMessageId.toString());
        }

        if (lastMessageId != null) {
            attributes.put(LAST_MESSAGE_ID_ATTRIBUTE, lastMessageId.toString());
        }

        for (Map.Entry<String, String> entry : commonProperties.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                attributes.put(PROPERTY_ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue());
            }
        }

        return attributes;
    }
}
