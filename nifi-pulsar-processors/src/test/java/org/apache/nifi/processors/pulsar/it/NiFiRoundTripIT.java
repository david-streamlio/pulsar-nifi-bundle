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
import java.util.List;
import java.util.TreeSet;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.testcontainers.containers.Container;

/**
 * NiFi in, NiFi out: what this bundle publishes, this bundle can read back.
 * <p>
 * Every other integration test here exercises one side and arranges or asserts the other with a plain
 * Pulsar client. That leaves the pairing itself untested, and the pairing is what a user actually
 * deploys. It is also where #181 lived: {@code PublishPulsar} creates producers with
 * {@code Schema.AUTO_PRODUCE_BYTES()}, which does <em>not</em> register a schema on a topic that had
 * none, so a topic written by this bundle is a schema-less topic - and {@code ConsumePulsarRecord}
 * threw {@code NullPointerException} on every message from one. A raw-client test on either side alone
 * could not see it; publishing with {@code Schema.BYTES} and consuming with {@code Schema.STRING} both
 * look fine in isolation.
 */
public class NiFiRoundTripIT extends AbstractPulsarIT {

    private static final int MESSAGES = 20;

    /** {@code PublishPulsar} to {@code ConsumePulsar}: the plainest pairing, raw bytes both ways. */
    @Test
    public void bytesSurviveTheRoundTrip() throws Exception {
        final String topic = topic("bytes");

        final TestRunner publisher = publisher(PublishPulsar.class, topic);
        for (int seq = 1; seq <= MESSAGES; seq++) {
            publisher.enqueue(("message-" + seq).getBytes(UTF_8));
        }
        publisher.run(MESSAGES, true);
        publisher.assertTransferCount(PublishPulsar.REL_SUCCESS, MESSAGES);

        assertNoSchemaWasRegistered(topic);

        final TestRunner consumer = TestRunners.newTestRunner(ConsumePulsar.class);
        addRealPulsarClientService(consumer, "pulsar-client");
        consumer.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        consumer.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        consumer.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "round-trip-bytes");
        consumer.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        consumer.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        consumer.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        consumer.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");

        final List<String> received = new ArrayList<>();
        consumeUntil(consumer, ConsumePulsar.REL_SUCCESS, () -> {
            received.clear();
            for (final MockFlowFile flowFile : consumer.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS)) {
                for (final String line : new String(flowFile.toByteArray(), UTF_8).split("\n")) {
                    if (!line.isEmpty()) {
                        received.add(line);
                    }
                }
            }
            return received.size();
        });

        assertEquals("every published message should come back", MESSAGES, received.size());
        assertEquals("and each exactly once", MESSAGES, new TreeSet<>(received).size());
    }

    /**
     * {@code PublishPulsar} to {@code ConsumePulsarRecord} over a topic this bundle created, which is the
     * exact shape of #181. Pins the fix end to end rather than through a mocked reader schema.
     */
    @Test
    public void recordsAreReadBackFromATopicThisBundleWrote() throws Exception {
        final String topic = topic("record");

        final TestRunner publisher = publisher(PublishPulsar.class, topic);
        for (int seq = 1; seq <= MESSAGES; seq++) {
            publisher.enqueue(("{\"device\":\"d" + (seq % 3) + "\",\"seq\":" + seq + "}").getBytes(UTF_8));
        }
        publisher.run(MESSAGES, true);
        publisher.assertTransferCount(PublishPulsar.REL_SUCCESS, MESSAGES);

        assertNoSchemaWasRegistered(topic);
        assertEquals(MESSAGES, consumeRecords(topic, "round-trip-record"));
    }

    /** {@code PublishPulsarRecord} to {@code ConsumePulsarRecord}: the record pairing end to end. */
    @Test
    public void recordsSurviveTheRecordToRecordRoundTrip() throws Exception {
        final String topic = topic("record-to-record");

        final TestRunner publisher = publisher(PublishPulsarRecord.class, topic);
        addRecordServices(publisher);
        publisher.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        publisher.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");

        final StringBuilder flowFileContent = new StringBuilder("[");
        for (int seq = 1; seq <= MESSAGES; seq++) {
            flowFileContent.append(seq > 1 ? "," : "")
                    .append("{\"device\":\"d").append(seq % 3).append("\",\"seq\":").append(seq).append("}");
        }
        publisher.enqueue(flowFileContent.append("]").toString().getBytes(UTF_8));
        publisher.run(1, true);
        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);

        assertEquals(MESSAGES, consumeRecords(topic, "round-trip-record-to-record"));
    }

    // ------------------------------------------------------------------ helpers

    private static String topic(final String name) {
        return "persistent://public/default/round-trip-" + name + "-" + System.nanoTime();
    }

    private TestRunner publisher(final Class<? extends org.apache.nifi.processor.Processor> processor,
            final String topic) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        return runner;
    }

    private void addRecordServices(final TestRunner runner) throws InitializationException {
        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);
    }

    /**
     * Asserts the topic carries no registered schema, which is what makes this the #181 shape.
     * {@code AUTO_PRODUCE_BYTES} validates against an existing schema but registers nothing, so a topic
     * this bundle wrote to has none unless something else put one there. If that ever changes, these
     * tests stop covering the case they were written for, and this says so rather than passing quietly.
     */
    private static void assertNoSchemaWasRegistered(final String topic) throws Exception {
        final Container.ExecResult result = PULSAR.execInContainer("bin/pulsar-admin", "schemas", "get", topic);
        final String output = result.getStdout() + result.getStderr();
        assertTrue("publishing through this bundle should leave the topic without a schema, but got:\n" + output,
                output.contains("Schema not found"));
    }

    /** Runs {@link ConsumePulsarRecord} over {@code topic} and returns how many records it produced. */
    private int consumeRecords(final String topic, final String subscription) throws Exception {
        final TestRunner consumer = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addRealPulsarClientService(consumer, "pulsar-client");
        addRecordServices(consumer);

        consumer.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        consumer.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        consumer.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        consumer.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        consumer.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, subscription);
        consumer.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        consumer.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        consumer.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        consumer.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "100");
        consumer.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");

        return consumeUntil(consumer, ConsumePulsarRecord.REL_SUCCESS, () -> {
            int records = 0;
            for (final MockFlowFile flowFile : consumer.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
                records += Integer.parseInt(flowFile.getAttribute("record.count"));
            }
            return records;
        });
    }

    /**
     * Triggers until {@code count} reports {@link #MESSAGES}, then stops the processor. The consumers poll
     * with no wait, so the first triggers after subscribing can legitimately return nothing while the
     * broker is still delivering - a fixed trigger count would "lose" a tail that had not arrived yet.
     */
    private static int consumeUntil(final TestRunner runner, final org.apache.nifi.processor.Relationship success,
            final java.util.concurrent.Callable<Integer> count) throws Exception {
        final int[] seen = {0};
        runner.run(1, false, true);
        await(MESSAGES + " messages to reach " + success.getName(), () -> {
            runner.run(1, false, false);
            seen[0] = count.call();
            return seen[0] >= MESSAGES;
        });
        runner.run(1, true, false);
        return seen[0];
    }
}
