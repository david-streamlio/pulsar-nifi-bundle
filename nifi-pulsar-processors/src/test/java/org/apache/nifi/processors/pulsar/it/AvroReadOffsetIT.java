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

import java.io.ByteArrayOutputStream;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

/**
 * Topics whose registered schema declares {@code __AVRO_READ_OFFSET__} (#207), read through a real broker.
 * <p>
 * The unit test in {@code TopicSchemaRecordDecoderTest} proves the decoder honours the property when it is
 * handed one. What it cannot prove is the part that involves the broker: that a schema carrying a
 * non-standard property survives registration, comes back on {@code AUTO_CONSUME}'s reader schema with the
 * property intact, and is therefore visible to the decoder at all. A property that the broker silently
 * dropped would leave the unit test passing and production still broken.
 * <p>
 * The framing here is written by hand rather than by a Debezium source: a magic byte and a four-byte
 * schema id ahead of the Avro body, which is what the Kafka Connect adaptor emits and why it sets the
 * property to 5. Confirming that a real Debezium source emits exactly this is not covered - that still
 * rests on reading the adaptor's code.
 */
public class AvroReadOffsetIT extends AbstractPulsarIT {

    private static final int RECORDS = 5;

    /** The Confluent wire format's preamble: one magic byte, then a four-byte big-endian schema id. */
    private static final int PREAMBLE = 5;

    private static final String SENSOR_FIELDS = "\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"reading\",\"type\":\"int\"}]";

    private static final String FRAMED_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Sensor\",\"__AVRO_READ_OFFSET__\":\"5\"," + SENSOR_FIELDS + "}";

    private static final String PLAIN_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Sensor\"," + SENSOR_FIELDS + "}";

    /**
     * The case from the issue: the topic's schema says the body starts five bytes in, and it does. Asserted
     * on decoded values rather than on the absence of a failure - decoding from byte zero yields a
     * plausible record rather than an error, so a test that only checked for parse failures would pass
     * against the bug.
     */
    @Test
    public void aFramedTopicIsDecodedPastItsPreamble() throws Exception {
        final String topic = registeredTopic("framed", FRAMED_SCHEMA);
        publishFramed(topic, PREAMBLE);

        final TestRunner consumer = consumer(topic, "framed");
        assertEquals(RECORDS, consumeRecords(consumer, RECORDS));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);

        final String content = successContent(consumer);
        for (int seq = 1; seq <= RECORDS; seq++) {
            assertTrue("record " + seq + " should have decoded its real id, got " + content,
                    content.contains("\"sensor-" + seq + "\""));
            assertTrue("and its real reading", content.contains("\"reading\":" + (seq * 10)));
        }
    }

    /**
     * The control, and the thing that must not regress: a topic whose schema declares no offset is still
     * read from byte zero. Almost every topic is this one.
     */
    @Test
    public void aTopicWithoutTheOffsetPropertyIsUnaffected() throws Exception {
        final String topic = registeredTopic("plain", PLAIN_SCHEMA);
        publishFramed(topic, 0);

        final TestRunner consumer = consumer(topic, "plain");
        assertEquals(RECORDS, consumeRecords(consumer, RECORDS));
        consumer.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        assertTrue(successContent(consumer).contains("\"sensor-1\""));
    }

    /**
     * The property has to survive the round trip through the schema registry to be of any use. Read back
     * from the broker rather than from the constant this test wrote, so a broker that dropped or rewrote
     * the property would fail here rather than silently reducing the test above to a no-op.
     */
    @Test
    public void theOffsetPropertySurvivesSchemaRegistration() throws Exception {
        final String topic = registeredTopic("survives", FRAMED_SCHEMA);

        // read back through the admin API rather than a consumer: a reader schema only appears once a
        // message has been received, and this asserts about registration itself
        final org.testcontainers.containers.Container.ExecResult result =
                PULSAR.execInContainer("bin/pulsar-admin", "schemas", "get", topic);
        final String output = result.getStdout() + result.getStderr();

        assertTrue("the broker should have kept __AVRO_READ_OFFSET__, but returned:\n" + output,
                output.contains("__AVRO_READ_OFFSET__"));
    }

    // ------------------------------------------------------------------ helpers

    /** Registers {@code definition} on a fresh topic by creating a producer that carries it. */
    private static String registeredTopic(final String name, final String definition) throws Exception {
        final String topic = "persistent://public/default/avro-offset-" + name + "-" + System.nanoTime();

        final SchemaInfo info = SchemaInfo.builder().name("Sensor").type(SchemaType.AVRO)
                .schema(definition.getBytes(UTF_8)).build();

        try (Producer<GenericRecord> registrar = getClient().newProducer(Schema.generic(info)).topic(topic).create()) {
            // creating the producer is what registers the schema; nothing needs to be sent through it
            registrar.getTopic();
        }

        return topic;
    }

    /**
     * Publishes {@link #RECORDS} Avro records with {@code preamble} bytes of framing ahead of each body,
     * through a plain bytes producer - the same shape the Kafka Connect adaptor puts on the wire.
     */
    private static void publishFramed(final String topic, final int preamble) throws Exception {
        final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(PLAIN_SCHEMA);

        try (Producer<byte[]> producer = getClient().newProducer(Schema.BYTES).topic(topic).create()) {
            for (int seq = 1; seq <= RECORDS; seq++) {
                final org.apache.avro.generic.GenericData.Record record =
                        new org.apache.avro.generic.GenericData.Record(avroSchema);
                record.put("id", "sensor-" + seq);
                record.put("reading", seq * 10);

                final ByteArrayOutputStream body = new ByteArrayOutputStream();
                final org.apache.avro.io.BinaryEncoder encoder =
                        org.apache.avro.io.EncoderFactory.get().binaryEncoder(body, null);
                new org.apache.avro.generic.GenericDatumWriter<org.apache.avro.generic.GenericRecord>(avroSchema)
                        .write(record, encoder);
                encoder.flush();

                final byte[] encoded = body.toByteArray();
                final byte[] framed = new byte[preamble + encoded.length];

                if (preamble >= PREAMBLE) {
                    framed[0] = 0x00;                       // magic byte
                    framed[4] = (byte) (seq & 0xFF);        // schema id, low byte varied per message
                }

                System.arraycopy(encoded, 0, framed, preamble, encoded.length);
                producer.send(framed);
            }
        }
    }

    private TestRunner consumer(final String topic, final String subscription) throws InitializationException {
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
