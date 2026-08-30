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

import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
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
 * NiFi in, NiFi out, once per schema type the bundle encodes - and what happens when the reader on the
 * way out does not match the encoding on the way in.
 * <p>
 * The two encodings are not interchangeable on the wire, which is the thing these tests exist to pin:
 * <ul>
 *   <li>an AVRO topic carries <em>bare Avro binary</em>. There is no file header and no embedded schema,
 *       because Pulsar keeps the schema in the registry, so a reader must be told the schema out of band.
 *       {@code ConsumePulsarRecord} publishes it as the {@code avro.schema} attribute for exactly this;
 *       an {@code AvroReader} pointed at {@code ${avro.schema}} can then decode it.</li>
 *   <li>a JSON topic carries JSON text, which {@code JsonTreeReader} reads with no schema at all.</li>
 * </ul>
 * {@code ConsumePulsarRecord} hands the raw message bytes to the configured reader rather than the
 * decoded value, so the reader has to match what the topic holds. Pointing the wrong one at a topic is a
 * realistic misconfiguration - the properties are set in different places by different people - and the
 * last two tests pin what happens then.
 * <p>
 * That behaviour is the processor's contract rather than an accident of the mismatch: every message is
 * routed to either {@code success} or {@code parse.failure} precisely so that it can be acknowledged, and
 * the {@code parse.failure} FlowFile is what carries the undecodable ones to their acknowledgement -
 * which is what #169 and #170 depend on. Nothing partially parsed reaches {@code success}, and the raw
 * payload stays recoverable from {@code parse.failure}.
 * <p>
 * Only AVRO and JSON are covered because only those two are encoded: {@code PublisherLease} returns no
 * topic schema for any other {@link SchemaType}, and records fall back to the configured Record Writer.
 */
public class NiFiSchemaRoundTripIT extends AbstractPulsarIT {

    private static final int RECORDS = 10;

    /**
     * Registering a schema on a topic means publishing to it, so every topic here starts with one seeded
     * record that the consumer reads along with the published ones. Its id is "seed" rather than
     * "sensor-N", so assertions that count published records can tell them apart.
     */
    private static final int SEEDED = 1;

    /** The schema every topic here carries, as an Avro schema document. Both types register this shape. */
    private static final String SENSOR_AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"Sensor\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"reading\",\"type\":\"int\"}]}";

    // ---------------------------------------------------------------- matching pairs

    /** AVRO out, AVRO back in, decoded with the schema the consumer reports on the FlowFile. */
    @Test
    public void anAvroTopicRoundTripsThroughAnAvroReader() throws Exception {
        final String topic = seededTopic("avro", SchemaType.AVRO);
        publishRecords(topic);

        final TestRunner consumer = consumer(topic, "avro-match", new AvroReader(), "${avro.schema}");
        assertEquals("every record should come back", RECORDS + SEEDED, consumeRecords(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);

        assertTrue("the consumer should report the topic's schema for an AVRO topic",
                consumer.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS).stream()
                        .allMatch(ff -> ff.getAttribute("avro.schema") != null
                                && ff.getAttribute("avro.schema").contains("Sensor")));
        assertTrue("and the decoded values should survive", successContent(consumer).contains("sensor-1"));
    }

    /** JSON out, JSON back in. The wire form is text, so no schema needs to reach the reader. */
    @Test
    public void aJsonTopicRoundTripsThroughAJsonReader() throws Exception {
        final String topic = seededTopic("json", SchemaType.JSON);
        publishRecords(topic);

        final TestRunner consumer = consumer(topic, "json-match", new JsonTreeReader(), null);
        assertEquals("every record should come back", RECORDS + SEEDED, consumeRecords(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);

        assertTrue("the decoded values should survive", successContent(consumer).contains("sensor-1"));
        assertTrue("avro.schema belongs to AVRO topics only",
                consumer.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS).stream()
                        .allMatch(ff -> ff.getAttribute("avro.schema") == null));
    }

    // ---------------------------------------------------------------- mismatched pairs

    /**
     * JSON on the topic, an AvroReader on the way out. The reader is given the topic's own schema, so the
     * only thing wrong is the encoding - JSON text where Avro binary is expected.
     */
    @Test
    public void jsonContentReadWithAnAvroReaderGoesToParseFailure() throws Exception {
        final String topic = seededTopic("json-to-avro", SchemaType.JSON);
        publishRecords(topic);

        final TestRunner consumer = consumer(topic, "json-to-avro", new AvroReader(), SENSOR_AVRO_SCHEMA);
        assertEquals("nothing should decode", 0, consumeParseFailures(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        assertTrue("the undecodable messages should be routed, not dropped or replayed forever",
                consumer.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE).size() > 0);
    }

    /** The same mistake the other way round: Avro binary on the topic, a JsonTreeReader on the way out. */
    @Test
    public void avroContentReadWithAJsonReaderGoesToParseFailure() throws Exception {
        final String topic = seededTopic("avro-to-json", SchemaType.AVRO);
        publishRecords(topic);

        final TestRunner consumer = consumer(topic, "avro-to-json", new JsonTreeReader(), null);
        assertEquals("nothing should decode", 0, consumeParseFailures(consumer));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 0);
        assertTrue("the undecodable messages should be routed, not dropped or replayed forever",
                consumer.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE).size() > 0);
    }

    // ------------------------------------------------------------------ helpers

    private static GenericSchema<GenericRecord> sensorSchema(final SchemaType type) {
        final RecordSchemaBuilder builder = SchemaBuilder.record("Sensor");
        builder.field("id").type(SchemaType.STRING);
        builder.field("reading").type(SchemaType.INT32);
        return Schema.generic(builder.build(type));
    }

    /** A topic carrying {@code type}'s schema, registered the way a schema-aware producer leaves it. */
    private static String seededTopic(final String name, final SchemaType type) throws Exception {
        final String topic = "persistent://public/default/schema-round-trip-" + name + "-" + System.nanoTime();
        final GenericSchema<GenericRecord> schema = sensorSchema(type);

        try (Producer<GenericRecord> seeder = getClient().newProducer(schema).topic(topic).create()) {
            seeder.send(schema.newRecordBuilder().set("id", "seed").set("reading", 0).build());
        }

        return topic;
    }

    /** Publishes {@link #RECORDS} records through {@link PublishPulsarRecord}, encoded with the topic's schema. */
    private void publishRecords(final String topic) throws Exception {
        final TestRunner publisher = TestRunners.newTestRunner(PublishPulsarRecord.class);
        addRealPulsarClientService(publisher, "pulsar-client");
        addService(publisher, "record-reader", new JsonTreeReader());
        addService(publisher, "record-writer", new JsonRecordSetWriter());

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

    private void addService(final TestRunner runner, final String id, final ControllerService service)
            throws InitializationException {
        runner.addControllerService(id, service);
        runner.enableControllerService(service);
    }

    private TestRunner consumer(final String topic, final String subscription, final ControllerService reader,
            final String avroSchemaText) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        runner.addControllerService("record-reader", reader);
        if (avroSchemaText != null) {
            // Pulsar writes bare Avro binary, so the schema has to reach the reader out of band. On an AVRO
            // topic that is ${avro.schema}, the attribute the consumer sets - resolving it here is what
            // proves the attribute is actually usable. A JSON topic has no such attribute, so the mismatch
            // test passes the schema literally and fails on the encoding rather than on a missing schema.
            runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "schema-text-property");
            runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, avroSchemaText);
        }
        runner.enableControllerService(reader);
        addService(runner, "record-writer", new JsonRecordSetWriter());

        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
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

    /**
     * Everything routed to success, concatenated. A record set is closed whenever the schema or the mapped
     * attributes change, so which FlowFile a given record lands in is a broker-timing detail; asserting on
     * the first one alone made this test depend on whether the seeded record arrived in its own batch.
     */
    private static String successContent(final TestRunner runner) {
        final StringBuilder content = new StringBuilder();
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
            content.append(new String(flowFile.toByteArray(), UTF_8));
        }
        return content.toString();
    }

    /** Triggers until {@link #RECORDS} records reach success, and returns how many did. */
    private static int consumeRecords(final TestRunner runner) throws Exception {
        final int[] records = {0};
        runner.run(1, false, true);
        await((RECORDS + SEEDED) + " records to be consumed", () -> {
            runner.run(1, false, false);
            records[0] = 0;
            for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
                records[0] += Integer.parseInt(flowFile.getAttribute("record.count"));
            }
            return records[0] >= RECORDS + SEEDED;
        });
        runner.run(1, true, false);
        return records[0];
    }

    /**
     * Triggers until every published message has been routed to {@code parse.failure}, and returns how many
     * records reached success - which should be none.
     * <p>
     * Undecodable messages are batched into a <em>single</em> demarcated FlowFile rather than one each, so
     * counting FlowFiles would say nothing about how many messages were accounted for. Both wire forms carry
     * the record ids as literal UTF-8 - Avro encodes strings unescaped - so the ids are countable in the
     * failure content either way, and that is the invariant worth asserting: every message that was
     * published came out somewhere, none silently dropped.
     */
    private static int consumeParseFailures(final TestRunner runner) throws Exception {
        runner.run(1, false, true);
        await("all " + RECORDS + " messages to reach parse.failure", () -> {
            runner.run(1, false, false);
            return routedIds(runner) >= RECORDS;
        });
        runner.run(1, true, false);

        assertEquals("every published message should be accounted for in parse.failure",
                RECORDS, routedIds(runner));

        int records = 0;
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
            records += Integer.parseInt(flowFile.getAttribute("record.count"));
        }
        return records;
    }

    /** How many published record ids appear across the parse.failure FlowFiles. */
    private static int routedIds(final TestRunner runner) {
        int found = 0;
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE)) {
            final String content = new String(flowFile.toByteArray(), UTF_8);
            int from = content.indexOf("sensor-");
            while (from >= 0) {
                found++;
                from = content.indexOf("sensor-", from + 1);
            }
        }
        return found;
    }
}
