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
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

/**
 * Topics carrying a key schema and a value schema (#190), end to end through NiFi.
 * <p>
 * The two encodings put the key in different places - {@code INLINE} length-prefixes it into the payload,
 * {@code SEPARATED} puts it in the message's key metadata - and {@code SEPARATED} is the one that makes a
 * topic compactable by key. Both are covered, because a flow that works on one and silently mangles the
 * other is exactly the failure this is meant to prevent.
 */
public class KeyValueSchemaRoundTripIT extends AbstractPulsarIT {

    private static final int RECORDS = 5;

    // ---------------------------------------------------------------- e2e round trips

    /** SEPARATED, the common shape: a STRING key in the message metadata, an AVRO value in the payload. */
    @Test
    public void aSeparatedKeyValueTopicRoundTripsThroughNiFi() throws Exception {
        final String topic = seededTopic("kv-separated", KeyValueEncodingType.SEPARATED, SchemaType.AVRO);

        publishWithNiFi(topic);

        final TestRunner consumer = topicSchemaConsumer(topic, "kv-separated");
        assertEquals(RECORDS + 1, consumeRecords(consumer, RECORDS + 1));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);

        final String content = successContent(consumer);
        assertTrue("the key should come back, from the message metadata: " + content, content.contains("device-1"));
        assertTrue("and the value with it", content.contains("\"reading\""));
    }

    /** INLINE, where both sides are length-prefixed into the payload instead. */
    @Test
    public void anInlineKeyValueTopicRoundTripsThroughNiFi() throws Exception {
        final String topic = seededTopic("kv-inline", KeyValueEncodingType.INLINE, SchemaType.AVRO);

        publishWithNiFi(topic);

        final TestRunner consumer = topicSchemaConsumer(topic, "kv-inline");
        assertEquals(RECORDS + 1, consumeRecords(consumer, RECORDS + 1));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);

        final String content = successContent(consumer);
        assertTrue("the key should come back, from the payload: " + content, content.contains("device-1"));
        assertTrue(content.contains("\"reading\""));
    }

    /** A JSON value side goes through the same path, so the encodings stay interchangeable to the user. */
    @Test
    public void aJsonValueSideRoundTrips() throws Exception {
        final String topic = seededTopic("kv-json", KeyValueEncodingType.SEPARATED, SchemaType.JSON);

        publishWithNiFi(topic);

        final TestRunner consumer = topicSchemaConsumer(topic, "kv-json");
        assertEquals(RECORDS + 1, consumeRecords(consumer, RECORDS + 1));
        assertTrue(successContent(consumer).contains("device-1"));
    }

    /** The field names are the user's, because they become column names downstream. */
    @Test
    public void theKeyAndValueFieldsAreNamedByTheProperties() throws Exception {
        final String topic = seededTopic("kv-named", KeyValueEncodingType.SEPARATED, SchemaType.AVRO);
        publishWithNiFi(topic);

        final TestRunner consumer = topicSchemaConsumer(topic, "kv-named");
        consumer.setProperty(ConsumePulsarRecord.KEY_VALUE_KEY_FIELD, "deviceId");
        consumer.setProperty(ConsumePulsarRecord.KEY_VALUE_VALUE_FIELD, "measurement");
        consumeRecords(consumer, RECORDS + 1);

        final String content = successContent(consumer);
        assertTrue("fields should be named as configured, got " + content, content.contains("\"deviceId\""));
        assertTrue(content.contains("\"measurement\""));
    }

    // ---------------------------------------------------------------- the msg.key collision

    /**
     * On a SEPARATED topic the schema owns the message key, so Message Key Field cannot also own it.
     * Refusing beats silently letting one overwrite the other - and because the topic's schema is only
     * known at publish time, this cannot be caught at validation.
     */
    @Test
    public void aDifferentMessageKeyFieldIsRefusedOnASeparatedTopic() throws Exception {
        final String topic = seededTopic("kv-key-clash", KeyValueEncodingType.SEPARATED, SchemaType.AVRO);

        final TestRunner publisher = publisher(topic);
        publisher.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "value");
        publisher.enqueue(recordsJson().getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 0);
        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 1);
    }

    /**
     * Naming the same field is not a conflict - it asks for exactly what the schema already guarantees -
     * so it publishes rather than failing, which would surprise anyone who set both for clarity.
     */
    @Test
    public void theSameMessageKeyFieldIsAllowedOnASeparatedTopic() throws Exception {
        final String topic = seededTopic("kv-key-same", KeyValueEncodingType.SEPARATED, SchemaType.AVRO);

        final TestRunner publisher = publisher(topic);
        publisher.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "key");
        publisher.enqueue(recordsJson().getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);
        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);

        final TestRunner consumer = topicSchemaConsumer(topic, "kv-key-same");
        assertEquals(RECORDS + 1, consumeRecords(consumer, RECORDS + 1));
        assertTrue(successContent(consumer).contains("device-1"));
    }

    /** A record missing either side cannot be mapped onto the topic's two schemas. */
    @Test
    public void aRecordWithoutBothSidesIsRefused() throws Exception {
        final String topic = seededTopic("kv-missing", KeyValueEncodingType.SEPARATED, SchemaType.AVRO);

        final TestRunner publisher = publisher(topic);
        publisher.enqueue("[{\"key\":\"device-1\"}]".getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 0);
        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 1);
    }

    // ------------------------------------------------------------------ helpers

    private static GenericSchema<GenericRecord> valueSchema(final SchemaType type) {
        final RecordSchemaBuilder builder = SchemaBuilder.record("Reading");
        builder.field("reading").type(SchemaType.INT32);
        return Schema.generic(builder.build(type));
    }

    /** A KeyValue topic with one record already on it, which is what registers the schema. */
    private static String seededTopic(final String name, final KeyValueEncodingType encoding,
            final SchemaType valueType) throws Exception {
        final String topic = "persistent://public/default/" + name + "-" + System.nanoTime();
        final GenericSchema<GenericRecord> values = valueSchema(valueType);
        final Schema<org.apache.pulsar.common.schema.KeyValue<String, GenericRecord>> schema =
                Schema.KeyValue(Schema.STRING, values, encoding);

        try (Producer<org.apache.pulsar.common.schema.KeyValue<String, GenericRecord>> seeder =
                     getClient().newProducer(schema).topic(topic).create()) {
            seeder.send(new org.apache.pulsar.common.schema.KeyValue<>("seed",
                    values.newRecordBuilder().set("reading", 0).build()));
        }

        return topic;
    }

    private static String recordsJson() {
        final StringBuilder content = new StringBuilder("[");
        for (int seq = 1; seq <= RECORDS; seq++) {
            content.append(seq > 1 ? "," : "")
                    .append("{\"key\":\"device-").append(seq).append("\",\"value\":{\"reading\":")
                    .append(seq * 10).append("}}");
        }
        return content.append("]").toString();
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

    private void publishWithNiFi(final String topic) throws Exception {
        final TestRunner publisher = publisher(topic);
        publisher.enqueue(recordsJson().getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);
        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
    }

    private TestRunner topicSchemaConsumer(final String topic, final String subscription)
            throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

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
