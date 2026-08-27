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

import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.FIRST_MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.LAST_MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.PROPERTY_ATTRIBUTE_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Test;

public class MessageBatchAttributesTest {

    private static Message<GenericRecord> message(String messageId, Map<String, String> properties) {
        return new MockPulsarMessage<>("test-topic", "payload".getBytes(), messageId, properties, null);
    }

    private static Map<String, String> properties(String... keyValues) {
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.put(keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }

    @Test
    public void noMessagesYieldNoAttributes() {
        MessageBatchAttributes batch = new MessageBatchAttributes();

        assertEquals(0, batch.getMessageCount());
        assertTrue(batch.toAttributes().isEmpty());
    }

    @Test
    public void singleMessageExposesItsIdAndAllOfItsProperties() {
        MessageBatchAttributes batch = new MessageBatchAttributes();
        batch.add(message("123:456:789", properties("source", "test-application", "version", "1.2.3")));

        Map<String, String> attributes = batch.toAttributes();

        assertEquals(1, batch.getMessageCount());
        assertEquals("123:456:789", attributes.get(MESSAGE_ID_ATTRIBUTE));
        assertEquals("123:456:789", attributes.get(FIRST_MESSAGE_ID_ATTRIBUTE));
        assertEquals("123:456:789", attributes.get(LAST_MESSAGE_ID_ATTRIBUTE));
        assertEquals("test-application", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "source"));
        assertEquals("1.2.3", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "version"));
        assertEquals(5, attributes.size());
    }

    @Test
    public void multipleMessagesExposeFirstAndLastIdButNoSingleId() {
        MessageBatchAttributes batch = new MessageBatchAttributes();
        batch.add(message("1:0:2", null));
        batch.add(message("1:1:2", null));
        batch.add(message("1:2:2", null));

        Map<String, String> attributes = batch.toAttributes();

        assertEquals(3, batch.getMessageCount());
        assertFalse("A multi-message FlowFile has no single message id", attributes.containsKey(MESSAGE_ID_ATTRIBUTE));
        assertEquals("1:0:2", attributes.get(FIRST_MESSAGE_ID_ATTRIBUTE));
        assertEquals("1:2:2", attributes.get(LAST_MESSAGE_ID_ATTRIBUTE));
        assertEquals(2, attributes.size());
    }

    @Test
    public void multipleMessagesKeepOnlyThePropertiesCommonToAllOfThem() {
        MessageBatchAttributes batch = new MessageBatchAttributes();
        batch.add(message("1:0:-1", properties("source", "app", "trace-id", "trace-1", "only-first", "x")));
        batch.add(message("1:1:-1", properties("source", "app", "trace-id", "trace-2")));
        batch.add(message("1:2:-1", properties("source", "app", "trace-id", "trace-3", "only-last", "y")));

        Map<String, String> attributes = batch.toAttributes();

        assertEquals("app", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "source"));
        assertFalse("A property whose value differs between messages is not published", attributes.containsKey(PROPERTY_ATTRIBUTE_PREFIX + "trace-id"));
        assertFalse("A property missing from some messages is not published", attributes.containsKey(PROPERTY_ATTRIBUTE_PREFIX + "only-first"));
        assertFalse("A property missing from some messages is not published", attributes.containsKey(PROPERTY_ATTRIBUTE_PREFIX + "only-last"));
    }

    @Test
    public void messageWithoutPropertiesClearsTheCommonProperties() {
        MessageBatchAttributes batch = new MessageBatchAttributes();
        batch.add(message("1:0:-1", properties("source", "app")));
        batch.add(message("1:1:-1", null));

        Map<String, String> attributes = batch.toAttributes();

        assertFalse(attributes.containsKey(PROPERTY_ATTRIBUTE_PREFIX + "source"));
        assertEquals("1:0:-1", attributes.get(FIRST_MESSAGE_ID_ATTRIBUTE));
        assertEquals("1:1:-1", attributes.get(LAST_MESSAGE_ID_ATTRIBUTE));
    }

    @Test
    public void nullMessageIdIsHandledGracefully() {
        MessageBatchAttributes batch = new MessageBatchAttributes();
        batch.add(message(null, properties("source", "app")));

        Map<String, String> attributes = batch.toAttributes();

        assertFalse(attributes.containsKey(MESSAGE_ID_ATTRIBUTE));
        assertFalse(attributes.containsKey(FIRST_MESSAGE_ID_ATTRIBUTE));
        assertFalse(attributes.containsKey(LAST_MESSAGE_ID_ATTRIBUTE));
        assertEquals("app", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "source"));
    }

    @Test
    public void nullAndEmptyPropertiesAreHandledGracefully() {
        MessageBatchAttributes nullProperties = new MessageBatchAttributes();
        nullProperties.add(message("1:0:-1", null));

        MessageBatchAttributes emptyProperties = new MessageBatchAttributes();
        emptyProperties.add(message("1:0:-1", new HashMap<>()));

        for (Map<String, String> attributes : new Map[] {nullProperties.toAttributes(), emptyProperties.toAttributes()}) {
            assertEquals("1:0:-1", attributes.get(MESSAGE_ID_ATTRIBUTE));
            for (String key : attributes.keySet()) {
                assertFalse("No property attribute expected but found " + key, key.startsWith(PROPERTY_ATTRIBUTE_PREFIX));
            }
        }
    }

    @Test
    public void specialCharactersInPropertyNamesArePreserved() {
        MessageBatchAttributes batch = new MessageBatchAttributes();
        batch.add(message("1:0:-1", properties(
                "property-with-dashes", "value1",
                "property.with.dots", "value2",
                "property_with_underscores", "value3",
                "property with spaces", "value4")));

        Map<String, String> attributes = batch.toAttributes();

        assertEquals("value1", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "property-with-dashes"));
        assertEquals("value2", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "property.with.dots"));
        assertEquals("value3", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "property_with_underscores"));
        assertEquals("value4", attributes.get(PROPERTY_ATTRIBUTE_PREFIX + "property with spaces"));
    }
}
