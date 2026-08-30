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

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Test;

/**
 * Topics whose schema is a single primitive value (#189), end to end through NiFi.
 * <p>
 * A primitive topic has one value per message and no fields, so the record shape is chosen rather than
 * derived: one field, named by <em>Primitive Value Field</em>. The interesting case is not the mapping
 * itself but the precedence rule around it - a STRING topic carrying JSON text is a common production
 * shape, and those flows want the payload parsed into records, not wrapped in a single string field. So a
 * configured Record Reader wins on a primitive topic, and the single-field record is what happens when
 * there is no reader to parse with.
 */
public class PrimitiveSchemaRoundTripIT extends AbstractPulsarIT {

    private static final int RECORDS = 5;

    // ---------------------------------------------------------------- e2e round trips

    /** NiFi in, NiFi out, over a STRING topic: publish single-field records and read them back. */
    @Test
    public void aStringTopicRoundTripsThroughNiFi() throws Exception {
        final String topic = seededStringTopic("string-e2e");

        publishWithNiFi(topic, "{\"value\":\"sensor-%d\"}");

        final TestRunner consumer = topicSchemaConsumer(topic, "string-e2e", null);
        assertEquals(RECORDS + 1, consumeRecords(consumer, RECORDS + 1));
        assertTrue("the published values should come back in the named field",
                successContent(consumer).contains("sensor-1"));
    }

    /** The same over an INT32 topic, where the value is 4 bytes rather than text. */
    @Test
    public void anInt32TopicRoundTripsThroughNiFi() throws Exception {
        final String topic = "persistent://public/default/int-e2e-" + System.nanoTime();

        try (Producer<Integer> seeder = getClient().newProducer(Schema.INT32).topic(topic).create()) {
            seeder.send(0);
        }

        publishWithNiFi(topic, "{\"value\":%d}");

        final TestRunner consumer = topicSchemaConsumer(topic, "int-e2e", null);
        assertEquals(RECORDS + 1, consumeRecords(consumer, RECORDS + 1));
        assertTrue("the numbers should survive as numbers", successContent(consumer).contains("\"value\":1"));
    }

    /** The field name is the user's, because it becomes a column name downstream. */
    @Test
    public void theValueFieldIsNamedByTheProperty() throws Exception {
        final String topic = seededStringTopic("named-field");
        publishWithNiFi(topic, "{\"value\":\"sensor-%d\"}");

        final TestRunner consumer = topicSchemaConsumer(topic, "named-field", null);
        consumer.setProperty(ConsumePulsarRecord.PRIMITIVE_VALUE_FIELD, "payload");
        consumeRecords(consumer, RECORDS + 1);

        final String content = successContent(consumer);
        assertTrue("the field should be named as configured, but got " + content, content.contains("\"payload\""));
    }

    // ---------------------------------------------------------------- precedence

    /**
     * The case from the review feedback on #184: a STRING topic whose messages are JSON text. With a
     * reader configured the payload is parsed into real records rather than wrapped, so the fields inside
     * the JSON are addressable downstream.
     */
    @Test
    public void aConfiguredReaderParsesTheStringPayloadInsteadOfWrappingIt() throws Exception {
        final String topic = "persistent://public/default/string-json-" + System.nanoTime();

        try (Producer<String> producer = getClient().newProducer(Schema.STRING).topic(topic).create()) {
            for (int seq = 1; seq <= RECORDS; seq++) {
                producer.send("{\"id\":\"sensor-" + seq + "\",\"reading\":" + (seq * 10) + "}");
            }
        }

        final TestRunner consumer = topicSchemaConsumer(topic, "string-json", new JsonTreeReader());
        assertEquals(RECORDS, consumeRecords(consumer, RECORDS));

        final String content = successContent(consumer);
        assertTrue("the JSON should have been parsed into fields, but got " + content, content.contains("\"reading\""));
        assertTrue(content.contains("\"id\""));
    }

    /** Without a reader there is nothing to parse with, so the value is wrapped rather than failed. */
    @Test
    public void withoutAReaderTheStringPayloadIsWrappedInTheValueField() throws Exception {
        final String topic = "persistent://public/default/string-wrap-" + System.nanoTime();

        try (Producer<String> producer = getClient().newProducer(Schema.STRING).topic(topic).create()) {
            for (int seq = 1; seq <= RECORDS; seq++) {
                producer.send("{\"id\":\"sensor-" + seq + "\"}");
            }
        }

        final TestRunner consumer = topicSchemaConsumer(topic, "string-wrap", null);
        assertEquals(RECORDS, consumeRecords(consumer, RECORDS));

        final String content = successContent(consumer);
        assertTrue("the payload should be one string field, but got " + content, content.contains("\"value\""));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
    }

    /**
     * The blind spot the review found, and the reason the behaviour is a property rather than an inference.
     * <p>
     * With a reader configured - which the README also advises, since the reader is the fallback for
     * schema-less topics - a STRING topic carrying plain text could only fail: the reader cannot parse
     * "hello world", and the single-field record was unreachable. Choosing 'Single-field record' decouples
     * the two decisions.
     */
    @Test
    public void plainTextIsWrappedWhenTheStrategySaysSoEvenWithAReaderConfigured() throws Exception {
        final String topic = "persistent://public/default/string-plain-" + System.nanoTime();

        try (Producer<String> producer = getClient().newProducer(Schema.STRING).topic(topic).create()) {
            producer.send("hello world");
            producer.send("second line");
        }

        final TestRunner consumer = topicSchemaConsumer(topic, "string-plain", new JsonTreeReader());
        consumer.setProperty(ConsumePulsarRecord.PRIMITIVE_SCHEMA_HANDLING, "Single-field record");

        assertEquals(2, consumeRecords(consumer, 2));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        assertTrue("the text should be wrapped in the value field", successContent(consumer).contains("hello world"));
    }

    /** The default is unchanged: with a reader configured, the reader still gets first refusal. */
    @Test
    public void theDefaultStillDefersToAConfiguredReader() throws Exception {
        final String topic = "persistent://public/default/string-default-" + System.nanoTime();

        try (Producer<String> producer = getClient().newProducer(Schema.STRING).topic(topic).create()) {
            for (int seq = 1; seq <= RECORDS; seq++) {
                producer.send("{\"id\":\"sensor-" + seq + "\",\"reading\":" + seq + "}");
            }
        }

        final TestRunner consumer = topicSchemaConsumer(topic, "string-default", new JsonTreeReader());
        assertEquals(RECORDS, consumeRecords(consumer, RECORDS));
        assertTrue("the reader should still have parsed the JSON", successContent(consumer).contains("\"reading\""));
    }

    // ---------------------------------------------------------------- publish-side guard

    /**
     * A primitive topic carries one value per message, so a record with several fields has no unambiguous
     * mapping. Guessing which field was meant would publish the wrong data silently, so it fails instead.
     */
    @Test
    public void aMultiFieldRecordCannotBePublishedToAPrimitiveTopic() throws Exception {
        final String topic = seededStringTopic("multi-field");

        final TestRunner publisher = publisher(topic);
        publisher.enqueue("[{\"id\":\"sensor-1\",\"reading\":10}]".getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 0);
        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 1);
    }

    /**
     * A topic with no schema is unaffected: the writer still serializes whatever it is given.
     * <p>
     * This is the test that caught BYTES being unusable as a primitive type. {@code AUTO_PRODUCE_BYTES}
     * reports a schema-less topic as {@code BYTES} with an empty definition, so treating BYTES as a
     * primitive made every schema-less topic reject multi-field records - and schema-less is what this
     * bundle's own publishers produce.
     */
    @Test
    public void aTopicWithoutASchemaIsUnaffected() throws Exception {
        final String topic = "persistent://public/default/prim-noschema-" + System.nanoTime();

        final TestRunner publisher = publisher(topic);
        publisher.enqueue("[{\"id\":\"sensor-1\",\"reading\":10}]".getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);

        try (Consumer<byte[]> consumer = getClient().newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName("prim-noschema").subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            final Message<byte[]> message = consumer.receive(30, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue("the record writer's output should be on the topic",
                    new String(message.getValue(), UTF_8).contains("sensor-1"));
        }
    }

    // ------------------------------------------------------------------ helpers

    /** A STRING topic with one record already on it, which is what registers the schema. */
    private static String seededStringTopic(final String name) throws Exception {
        final String topic = "persistent://public/default/prim-" + name + "-" + System.nanoTime();

        try (Producer<String> seeder = getClient().newProducer(Schema.STRING).topic(topic).create()) {
            seeder.send("seed");
        }

        return topic;
    }

    private TestRunner publisher(final String topic) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishPulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(PublishPulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");
        return runner;
    }

    /** Publishes {@link #RECORDS} single-field records, each built from {@code template}. */
    private void publishWithNiFi(final String topic, final String template) throws Exception {
        final TestRunner publisher = publisher(topic);

        final StringBuilder content = new StringBuilder("[");
        for (int seq = 1; seq <= RECORDS; seq++) {
            content.append(seq > 1 ? "," : "").append(String.format(template, seq));
        }
        publisher.enqueue(content.append("]").toString().getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);
        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
    }

    private TestRunner topicSchemaConsumer(final String topic, final String subscription,
            final JsonTreeReader reader) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        if (reader != null) {
            runner.addControllerService("record-reader", reader);
            runner.enableControllerService(reader);
            runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        }

        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(ConsumePulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, subscription);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "100");
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
        return runner;
    }

    private static String successContent(final TestRunner runner) {
        final StringBuilder content = new StringBuilder();
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
            content.append(new String(flowFile.toByteArray(), UTF_8));
        }
        return content.toString();
    }

    private static int consumeRecords(final TestRunner runner, final int expected) throws Exception {
        final int[] records = {0};
        runner.run(1, false, true);
        await(expected + " records to be consumed", () -> {
            runner.run(1, false, false);
            records[0] = 0;
            for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
                records[0] += Integer.parseInt(flowFile.getAttribute("record.count"));
            }
            return records[0] >= expected;
        });
        runner.run(1, true, false);
        return records[0];
    }
}
