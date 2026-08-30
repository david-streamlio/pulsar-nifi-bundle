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
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

/**
 * Consuming with the topic's own schema instead of a configured Record Reader (#185).
 * <p>
 * The field definitions come from the broker: Pulsar keeps schemas in a registry keyed by topic and
 * version, and {@code AUTO_CONSUME} attaches each message's schema as its reader schema. So the strategy
 * needs no schema configuration of any kind - notably not the {@code AvroReader} with Schema Text
 * {@code ${avro.schema}} that an AVRO topic otherwise requires, which is the configuration this replaces.
 * <p>
 * The e2e pairing - publish with this bundle, read it back with this bundle, on both encodings - is in
 * {@link NiFiSchemaRoundTripIT}.
 */
public class ConsumePulsarTopicSchemaIT extends AbstractPulsarIT {

    private static final int RECORDS = 10;

    /** AVRO is the case that motivated this: bare binary that no reader can parse unaided. */
    @Test
    public void anAvroTopicIsConsumedWithNoRecordReaderConfigured() throws Exception {
        final String topic = seededTopic("avro-topic-schema", SchemaType.AVRO);
        publishWithPlainClient(topic, SchemaType.AVRO);

        final TestRunner consumer = topicSchemaConsumer(topic, "avro-ts");
        assertEquals(RECORDS + 1, consumeRecords(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        assertTrue("the decoded field values should be present", successContent(consumer).contains("sensor-1"));
    }

    /** JSON goes through the same strategy, so the two encodings become indistinguishable to the user. */
    @Test
    public void aJsonTopicIsConsumedWithNoRecordReaderConfigured() throws Exception {
        final String topic = seededTopic("json-topic-schema", SchemaType.JSON);
        publishWithPlainClient(topic, SchemaType.JSON);

        final TestRunner consumer = topicSchemaConsumer(topic, "json-ts");
        assertEquals(RECORDS + 1, consumeRecords(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        assertTrue("the decoded field values should be present", successContent(consumer).contains("sensor-1"));
    }

    /**
     * A schema-less topic has no definition to decode with, so the strategy falls back to the Record
     * Reader. This is the #181 shape, and it is why the reader stays configurable under this strategy.
     */
    @Test
    public void aTopicWithoutASchemaFallsBackToTheRecordReader() throws Exception {
        final String topic = "persistent://public/default/ts-noschema-" + System.nanoTime();
        publish(topic, jsonPayloads());

        final TestRunner consumer = topicSchemaConsumer(topic, "noschema-ts");
        final JsonTreeReader reader = new JsonTreeReader();
        consumer.addControllerService("record-reader", reader);
        consumer.enableControllerService(reader);
        consumer.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");

        assertEquals("the reader should have handled what the topic schema could not",
                RECORDS, consumeRecordsExpecting(consumer, RECORDS));
    }

    /** Without a reader to fall back to, those messages are routed rather than silently dropped. */
    @Test
    public void aTopicWithoutASchemaAndNoReaderGoesToParseFailure() throws Exception {
        final String topic = "persistent://public/default/ts-noschema-noreader-" + System.nanoTime();
        publish(topic, jsonPayloads());

        final TestRunner consumer = topicSchemaConsumer(topic, "noschema-noreader-ts");
        consumer.run(1, false, true);
        await("messages to reach parse.failure", () -> {
            consumer.run(1, false, false);
            return !consumer.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE).isEmpty();
        });
        consumer.run(1, true, false);

        consumer.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
    }

    // ------------------------------------------------------------------ e2e: NiFi in, NiFi out

    /**
     * The pairing a user actually deploys: publish with {@code PublishPulsarRecord} on 'Topic Schema' and
     * read it back with {@code ConsumePulsarRecord} on 'Topic Schema', with no Record Reader anywhere in
     * the flow. AVRO is the half that otherwise needs an AvroReader pointed at {@code ${avro.schema}}.
     */
    @Test
    public void anAvroTopicRoundTripsThroughNiFiOnTheTopicSchemaAlone() throws Exception {
        final String topic = seededTopic("avro-e2e", SchemaType.AVRO);
        publishWithNiFi(topic);

        final TestRunner consumer = topicSchemaConsumer(topic, "avro-e2e");
        assertEquals(RECORDS + 1, consumeRecords(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        assertTrue("the decoded values should survive the round trip", successContent(consumer).contains("sensor-1"));
    }

    /** The same on JSON, so the two encodings are indistinguishable once the topic's schema drives both ends. */
    @Test
    public void aJsonTopicRoundTripsThroughNiFiOnTheTopicSchemaAlone() throws Exception {
        final String topic = seededTopic("json-e2e", SchemaType.JSON);
        publishWithNiFi(topic);

        final TestRunner consumer = topicSchemaConsumer(topic, "json-e2e");
        assertEquals(RECORDS + 1, consumeRecords(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        assertTrue("the decoded values should survive the round trip", successContent(consumer).contains("sensor-1"));
    }

    // ------------------------------------------------------------------ helpers

    /** Publishes {@link #RECORDS} records through {@code PublishPulsarRecord}, encoded with the topic's schema. */
    private void publishWithNiFi(final String topic) throws Exception {
        final TestRunner publisher = TestRunners.newTestRunner(PublishPulsarRecord.class);
        addRealPulsarClientService(publisher, "pulsar-client");

        final JsonTreeReader reader = new JsonTreeReader();
        publisher.addControllerService("record-reader", reader);
        publisher.enableControllerService(reader);
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        publisher.addControllerService("record-writer", writer);
        publisher.enableControllerService(writer);

        publisher.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        publisher.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        publisher.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        publisher.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        publisher.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        publisher.setProperty(PublishPulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");

        final StringBuilder content = new StringBuilder("[");
        for (int seq = 1; seq <= RECORDS; seq++) {
            content.append(seq > 1 ? "," : "")
                    .append("{\"id\":\"sensor-").append(seq).append("\",\"reading\":").append(seq * 10).append("}");
        }
        publisher.enqueue(content.append("]").toString().getBytes(UTF_8));
        publisher.run(1, true);

        publisher.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
        publisher.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);
    }

    private static GenericSchema<GenericRecord> sensorSchema(final SchemaType type) {
        final RecordSchemaBuilder builder = SchemaBuilder.record("Sensor");
        builder.field("id").type(SchemaType.STRING);
        builder.field("reading").type(SchemaType.INT32);
        return Schema.generic(builder.build(type));
    }

    private static String seededTopic(final String name, final SchemaType type) throws Exception {
        final String topic = "persistent://public/default/" + name + "-" + System.nanoTime();
        final GenericSchema<GenericRecord> schema = sensorSchema(type);

        try (Producer<GenericRecord> seeder = getClient().newProducer(schema).topic(topic).create()) {
            seeder.send(schema.newRecordBuilder().set("id", "seed").set("reading", 0).build());
        }

        return topic;
    }

    /** Publishes with a plain schema-aware client, so the consumer is tested independently of our publisher. */
    private static void publishWithPlainClient(final String topic, final SchemaType type) throws Exception {
        final GenericSchema<GenericRecord> schema = sensorSchema(type);

        try (Producer<GenericRecord> producer = getClient().newProducer(schema).topic(topic).create()) {
            for (int seq = 1; seq <= RECORDS; seq++) {
                producer.send(schema.newRecordBuilder().set("id", "sensor-" + seq).set("reading", seq * 10).build());
            }
        }
    }

    private static String[] jsonPayloads() {
        final String[] payloads = new String[RECORDS];
        for (int seq = 1; seq <= RECORDS; seq++) {
            payloads[seq - 1] = "{\"id\":\"sensor-" + seq + "\",\"reading\":" + (seq * 10) + "}";
        }
        return payloads;
    }

    /** A consumer on the Topic Schema strategy, deliberately with no Record Reader configured. */
    private TestRunner topicSchemaConsumer(final String topic, final String subscription) throws InitializationException {
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

    private static int consumeRecords(final TestRunner runner) throws Exception {
        return consumeRecordsExpecting(runner, RECORDS + 1);
    }

    private static int consumeRecordsExpecting(final TestRunner runner, final int expected) throws Exception {
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
