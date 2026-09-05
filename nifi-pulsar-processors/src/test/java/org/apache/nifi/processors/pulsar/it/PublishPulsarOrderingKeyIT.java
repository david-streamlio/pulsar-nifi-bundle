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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.Test;

/**
 * The ordering key against a real broker: it has to reach the message, and on a {@code Key_Shared}
 * subscription it has to be what decides which consumer receives the message - the message key routes and
 * compacts, the ordering key orders, and when both are set the broker hashes the ordering key.
 * <p>
 * The dispatch test publishes messages that all carry <b>distinct</b> message keys and <b>one</b> ordering key
 * to a two-consumer Key_Shared subscription. Without an ordering key the distinct message keys spread the
 * messages over both consumers; with it, every message lands on the same consumer, in publish order. Batching
 * is off so each message is dispatched on its own key rather than as part of a mixed-key batch.
 */
public class PublishPulsarOrderingKeyIT extends AbstractPulsarIT {

    private static final int MESSAGES = 40;

    private TestRunner publishPulsar() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishPulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHING_ENABLED, "false");
        return runner;
    }

    private TestRunner publishPulsarRecord() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishPulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("tenant", RecordFieldType.STRING);
        reader.addSchemaField("session", RecordFieldType.STRING);
        reader.addSchemaField("seq", RecordFieldType.INT);
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);

        final MockRecordWriter writer = new MockRecordWriter("tenant, session, seq");
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHING_ENABLED, "false");
        return runner;
    }

    private static String topic(final String name) {
        return "persistent://public/default/ordering-key-" + name + "-" + System.nanoTime();
    }

    @Test
    public void theOrderingKeyReachesTheMessageAlongsideTheMessageKey() throws Exception {
        final String topic = topic("plain");
        final TestRunner runner = publishPulsar();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_KEY, "tenant-a");
        runner.setProperty(AbstractPulsarProducerProcessor.ORDERING_KEY, "${session}");

        try (Consumer<byte[]> consumer = subscribe(topic, "plain-check", SubscriptionType.Exclusive)) {
            runner.enqueue("payload".getBytes(UTF_8), java.util.Map.of("session", "session-7"));
            runner.run(1, true);
            runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);

            final Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals("tenant-a", message.getKey());
            assertTrue("the message must carry an ordering key", message.hasOrderingKey());
            assertArrayEquals("session-7".getBytes(UTF_8), message.getOrderingKey());
        }
    }

    /** Unset is the existing behaviour: no ordering key on the message, so Pulsar falls back to the message key. */
    @Test
    public void noOrderingKeyIsSetWhenThePropertyIsUnset() throws Exception {
        final String topic = topic("unset");
        final TestRunner runner = publishPulsar();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_KEY, "tenant-a");

        try (Consumer<byte[]> consumer = subscribe(topic, "unset-check", SubscriptionType.Exclusive)) {
            runner.enqueue("payload".getBytes(UTF_8));
            runner.run(1, true);

            final Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals("tenant-a", message.getKey());
            assertFalse(message.hasOrderingKey());
        }
    }

    /**
     * Distinct message keys, one ordering key: Key_Shared must hash the ordering key, so a single consumer
     * receives everything, in order. The control run - same message keys, no ordering key - has to spread over
     * both consumers, or the assertion above would also hold for a broker that ignored the ordering key.
     */
    @Test
    public void keySharedDispatchFollowsTheOrderingKeyNotTheMessageKey() throws Exception {
        // control: no ordering key, distinct message keys spread over both consumers
        final String controlTopic = topic("control");
        final TestRunner control = publishPulsar();
        control.setProperty(AbstractPulsarProducerProcessor.TOPIC, controlTopic);
        control.setProperty(AbstractPulsarProducerProcessor.MESSAGE_KEY, "${key}");
        final Distribution spread = publishToTwoKeySharedConsumers(control, controlTopic, false);
        assertTrue("control: " + MESSAGES + " distinct message keys with no ordering key should reach both "
                + "consumers, but landed " + spread, spread.first > 0 && spread.second > 0);

        // the same distinct message keys, plus one ordering key for all of them
        final String topic = topic("dispatch");
        final TestRunner runner = publishPulsar();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_KEY, "${key}");
        runner.setProperty(AbstractPulsarProducerProcessor.ORDERING_KEY, "session-1");
        final Distribution together = publishToTwoKeySharedConsumers(runner, topic, true);
        assertTrue("one ordering key must keep every message on one consumer, but they landed " + together,
                together.first == 0 || together.second == 0);
        assertEquals("every message must have arrived", MESSAGES, together.first + together.second);
        assertTrue("messages sharing the ordering key must arrive in publish order", together.inOrder);
    }

    @Test
    public void publishPulsarRecordTakesTheOrderingKeyFromARecordField() throws Exception {
        final String topic = topic("record");
        final TestRunner runner = publishPulsarRecord();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "tenant");
        runner.setProperty(PublishPulsarRecord.ORDERING_KEY_FIELD, "session");

        try (Consumer<byte[]> consumer = subscribe(topic, "record-check", SubscriptionType.Exclusive)) {
            runner.enqueue("acme,s-1,1\nglobex,s-2,2".getBytes(UTF_8));
            runner.run(1, true);
            runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);

            final Message<byte[]> first = consumer.receive(30, TimeUnit.SECONDS);
            final Message<byte[]> second = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(first);
            assertNotNull(second);
            assertEquals("acme", first.getKey());
            assertArrayEquals("s-1".getBytes(UTF_8), first.getOrderingKey());
            assertEquals("globex", second.getKey());
            assertArrayEquals("s-2".getBytes(UTF_8), second.getOrderingKey());
        }
    }

    private Consumer<byte[]> subscribe(final String topic, final String subscription, final SubscriptionType type)
            throws Exception {
        return getClient().newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionType(type)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
    }

    private static final class Distribution {
        int first;
        int second;
        boolean inOrder = true;

        @Override
        public String toString() {
            return first + " on the first consumer and " + second + " on the second";
        }
    }

    /**
     * Publishes {@link #MESSAGES} FlowFiles whose {@code key} attribute is distinct per message, then drains a
     * two-consumer Key_Shared subscription and reports how the messages were distributed.
     */
    private Distribution publishToTwoKeySharedConsumers(final TestRunner runner, final String topic,
                                                        final boolean expectOrderingKey) throws Exception {
        try (Consumer<byte[]> first = subscribe(topic, "key-shared", SubscriptionType.Key_Shared);
             Consumer<byte[]> second = subscribe(topic, "key-shared", SubscriptionType.Key_Shared)) {

            for (int n = 0; n < MESSAGES; n++) {
                runner.enqueue(String.valueOf(n).getBytes(UTF_8), java.util.Map.of("key", "device-" + n));
            }
            runner.run(MESSAGES, true);
            runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, MESSAGES);

            // Everything is published by now, so each consumer is drained in turn until it has been quiet for
            // a second; the outer loop only repeats if the broker was still dispatching.
            final Distribution distribution = new Distribution();
            final List<Integer> firstOrder = new ArrayList<>();
            final List<Integer> secondOrder = new ArrayList<>();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (firstOrder.size() + secondOrder.size() < MESSAGES && System.nanoTime() < deadline) {
                drain(first, firstOrder, expectOrderingKey);
                drain(second, secondOrder, expectOrderingKey);
            }
            distribution.first = firstOrder.size();
            distribution.second = secondOrder.size();
            distribution.inOrder = isAscending(firstOrder) && isAscending(secondOrder);
            return distribution;
        }
    }

    private static void drain(final Consumer<byte[]> consumer, final List<Integer> received, final boolean expectOrderingKey)
            throws Exception {
        Message<byte[]> message;
        while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
            received.add(Integer.parseInt(new String(message.getValue(), UTF_8)));
            assertEquals(expectOrderingKey, message.hasOrderingKey());
            consumer.acknowledge(message);
        }
    }

    private static boolean isAscending(final List<Integer> values) {
        for (int i = 1; i < values.size(); i++) {
            if (values.get(i) < values.get(i - 1)) {
                return false;
            }
        }
        return true;
    }
}
