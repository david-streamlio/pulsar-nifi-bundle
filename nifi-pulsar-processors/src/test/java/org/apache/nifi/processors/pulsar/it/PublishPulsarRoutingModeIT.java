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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Test;
import org.testcontainers.containers.Container;

/**
 * Where messages land on a partitioned topic is decided by the broker and the client's router, so the
 * routing mode can only be verified against a real broker.
 * <p>
 * Every case publishes one FlowFile of 30 demarcated messages to a topic with three partitions and reads
 * them back with a plain Pulsar consumer, which reports the partition each message came from.
 */
public class PublishPulsarRoutingModeIT extends AbstractPulsarIT {

    private static final int PARTITIONS = 3;
    private static final int MESSAGES = 30;

    /** The case from the issue: SinglePartition has to keep unkeyed messages together. */
    @Test
    public void singlePartitionKeepsUnkeyedMessagesOnOnePartition() throws Exception {
        final String topic = createPartitionedTopic();
        final TestRunner runner = publisher(topic, "SinglePartition");

        try (Consumer<byte[]> consumer = subscribe(topic)) {
            runner.enqueue(unkeyedMessages());
            runner.run(1, true);
            runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);

            final List<Message<byte[]>> received = receive(consumer, MESSAGES);
            assertEquals(MESSAGES, received.size());
            assertEquals("SinglePartition must route every unkeyed message to the same partition, but they arrived on "
                    + partitionsOf(received), 1, partitionsOf(received).size());
        }
    }

    /** The default, which the broken code applied by accident: unkeyed messages go round the partitions. */
    @Test
    public void roundRobinSpreadsUnkeyedMessagesOverEveryPartition() throws Exception {
        final String topic = createPartitionedTopic();
        final TestRunner runner = publisher(topic, "RoundRobinPartition");

        try (Consumer<byte[]> consumer = subscribe(topic)) {
            runner.enqueue(unkeyedMessages());
            runner.run(1, true);
            runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);

            final List<Message<byte[]>> received = receive(consumer, MESSAGES);
            assertEquals(MESSAGES, received.size());
            assertEquals("round robin should use every partition, but used " + partitionsOf(received),
                    PARTITIONS, partitionsOf(received).size());
        }
    }

    /** A key is hashed to a partition in every mode, so keyed messages keep their partition and order. */
    @Test
    public void keyedMessagesStayOnOnePartitionPerKeyInEitherMode() throws Exception {
        for (final String mode : new String[] {"RoundRobinPartition", "SinglePartition"}) {
            final String topic = createPartitionedTopic();
            final TestRunner runner = publisher(topic, mode);

            try (Consumer<byte[]> consumer = subscribe(topic)) {
                runner.enqueue(unkeyedMessages(), Collections.singletonMap(AbstractPulsarProducerProcessor.MSG_KEY, "device-7"));
                runner.run(1, true);
                runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);

                final List<Message<byte[]>> received = receive(consumer, MESSAGES);
                assertEquals(MESSAGES, received.size());
                assertEquals(mode + ": one key must map to one partition, got " + partitionsOf(received), 1, partitionsOf(received).size());

                int last = 0;
                for (final Message<byte[]> message : received) {
                    final int seq = Integer.parseInt(new String(message.getValue(), UTF_8).replaceAll("\\D", ""));
                    assertTrue(mode + ": out of order, " + last + " then " + seq, seq > last);
                    last = seq;
                }
            }
        }
    }

    private static int topicCounter = 0;

    private static String createPartitionedTopic() throws Exception {
        final String topic = "persistent://public/default/routing-" + (topicCounter++) + "-" + System.nanoTime();
        final Container.ExecResult result = PULSAR.execInContainer("bin/pulsar-admin", "topics", "create-partitioned-topic",
                topic, "-p", String.valueOf(PARTITIONS));
        assertEquals("pulsar-admin failed: " + result.getStderr(), 0, result.getExitCode());
        return topic;
    }

    /** Batching is off so the round-robin router rotates per message rather than per batching window. */
    private TestRunner publisher(final String topic, final String routingMode) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishPulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarProducerProcessor.BATCHING_ENABLED, "false");
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_ROUTING_MODE, routingMode);
        return runner;
    }

    private static byte[] unkeyedMessages() {
        final StringBuilder content = new StringBuilder();
        for (int seq = 1; seq <= MESSAGES; seq++) {
            content.append("m").append(seq).append('\n');
        }
        return content.toString().getBytes(UTF_8);
    }

    private static Consumer<byte[]> subscribe(final String topic) throws Exception {
        return getClient().newConsumer(Schema.BYTES).topic(topic).subscriptionName("routing-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
    }

    private static List<Message<byte[]>> receive(final Consumer<byte[]> consumer, final int expected) throws Exception {
        final List<Message<byte[]>> messages = new ArrayList<>();
        while (messages.size() < expected) {
            final Message<byte[]> message = consumer.receive(15, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            messages.add(message);
            consumer.acknowledge(message);
        }
        return messages;
    }

    /** The partitions the messages arrived on, e.g. {@code [partition-0, partition-2]}. */
    private static TreeSet<String> partitionsOf(final List<Message<byte[]>> messages) {
        final TreeSet<String> partitions = new TreeSet<>();
        for (final Message<byte[]> message : messages) {
            final String topic = message.getTopicName();
            partitions.add(topic.substring(topic.lastIndexOf("-partition-") + 1));
        }
        return partitions;
    }

}
