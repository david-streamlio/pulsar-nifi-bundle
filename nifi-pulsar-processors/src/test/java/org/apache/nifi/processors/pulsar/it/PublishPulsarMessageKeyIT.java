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
package org.apache.nifi.processors.pulsar.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.junit.Before;
import org.junit.Test;

/**
 * What actually lands on the topic when PublishPulsar sends: key, properties and content.
 * <p>
 * None of this had real-broker coverage, which is how the documented {@code msg.key} fallback stayed
 * unimplemented. The Message Key property has always said "if not specified, the flow file attribute
 * 'msg.key' is used as the message key, if it is present", but {@code getMessageKey()} only read the
 * property and returned null otherwise, so the message went out unkeyed - silently changing partition
 * routing and making the topic uncompactable by key.
 */
public class PublishPulsarMessageKeyIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    /** The documented fallback: no Message Key property, but a msg.key attribute on the FlowFile. */
    @Test
    public void theMsgKeyAttributeIsUsedWhenThePropertyIsNotSet() throws Exception {
        final String topic = topic("attr-key");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);

        try (Consumer<byte[]> consumer = subscribe(topic)) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(AbstractPulsarProducerProcessor.MSG_KEY, "order-42");
            runner.enqueue("payload".getBytes(UTF_8), attributes);
            runner.run(1, true);
            runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);

            final Message<byte[]> message = receive(consumer);
            assertEquals("order-42", message.getKey());
            assertEquals("payload", new String(message.getValue(), UTF_8));
        }
    }

    /** An explicit property still wins over the attribute. */
    @Test
    public void theMessageKeyPropertyTakesPrecedence() throws Exception {
        final String topic = topic("prop-key");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_KEY, "from-property");

        try (Consumer<byte[]> consumer = subscribe(topic)) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(AbstractPulsarProducerProcessor.MSG_KEY, "from-attribute");
            runner.enqueue("payload".getBytes(UTF_8), attributes);
            runner.run(1, true);

            assertEquals("from-property", receive(consumer).getKey());
        }
    }

    /** With neither set, the message stays unkeyed - the fallback must not invent one. */
    @Test
    public void aMessageWithNeitherIsUnkeyed() throws Exception {
        final String topic = topic("no-key");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);

        try (Consumer<byte[]> consumer = subscribe(topic)) {
            runner.enqueue("payload".getBytes(UTF_8));
            runner.run(1, true);

            assertNull(receive(consumer).getKey());
        }
    }

    /** Mapped message properties are the other half of the metadata contract, equally unverified. */
    @Test
    public void mappedAttributesBecomeMessageProperties() throws Exception {
        final String topic = topic("props");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.MAPPED_MESSAGE_PROPERTIES, "source,tenant=org.id");

        try (Consumer<byte[]> consumer = subscribe(topic)) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("source", "device-gateway");
            attributes.put("org.id", "acme");
            runner.enqueue("payload".getBytes(UTF_8), attributes);
            runner.run(1, true);

            final Message<byte[]> message = receive(consumer);
            assertEquals("device-gateway", message.getProperty("source"));
            assertEquals("acme", message.getProperty("tenant"));
        }
    }

    // ------------------------------------------------------------------ helpers

    private static String topic(final String name) {
        return "persistent://public/default/" + name + "-" + System.nanoTime();
    }

    /** Subscribes before publishing so the message cannot be missed by a late subscription. */
    private static Consumer<byte[]> subscribe(final String topic) throws Exception {
        return getClient().newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("key-check")
                .subscribe();
    }

    private static Message<byte[]> receive(final Consumer<byte[]> consumer) throws Exception {
        final Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
        assertNotNull("no message arrived within 30s", message);
        return message;
    }
}
