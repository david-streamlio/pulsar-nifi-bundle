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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

/** Decoding a message with the schema its topic carries, independently of a broker. */
public class TopicSchemaRecordDecoderTest {

    private static final String SENSOR = "{\"type\":\"record\",\"name\":\"Sensor\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"reading\",\"type\":\"int\"}]}";

    private final TopicSchemaRecordDecoder decoder = new TopicSchemaRecordDecoder();

    /** AVRO topics carry bare binary, so the schema has to come from the SchemaInfo rather than the data. */
    @Test
    public void avroBytesDecodeToARecord() throws Exception {
        final Record record = decoder.decode(avroBytes("sensor-1", 42), schemaInfo(SchemaType.AVRO));

        assertEquals("sensor-1", record.getValue("id"));
        assertEquals(42, record.getValue("reading"));
    }

    /** JSON topics carry plain text, and the schema still decides the field types. */
    @Test
    public void jsonBytesDecodeToARecord() throws Exception {
        final Record record = decoder.decode("{\"id\":\"sensor-2\",\"reading\":43}".getBytes(UTF_8),
                schemaInfo(SchemaType.JSON));

        assertEquals("sensor-2", record.getValue("id"));
        assertEquals(43, record.getValue("reading"));
    }

    /**
     * JSON cannot distinguish an int from a long, so the value arrives as whatever Jackson inferred and has
     * to be coerced to the schema's type. Without coercion a downstream writer expecting an int gets a
     * Long, which is the kind of mismatch that only shows up once the data reaches a typed sink.
     */
    @Test
    public void jsonValuesAreCoercedToTheSchemaTypes() throws Exception {
        final Record record = decoder.decode("{\"id\":\"sensor-3\",\"reading\":44}".getBytes(UTF_8),
                schemaInfo(SchemaType.JSON));

        assertTrue("reading should be an Integer, not a " + record.getValue("reading").getClass().getSimpleName(),
                record.getValue("reading") instanceof Integer);
    }

    /** The parsed schema is reused between messages, but a changed definition must not be served stale. */
    @Test
    public void aChangedSchemaDefinitionIsReparsed() throws Exception {
        decoder.decode(avroBytes("sensor-1", 42), schemaInfo(SchemaType.AVRO));

        final String evolved = "{\"type\":\"record\",\"name\":\"Sensor\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"reading\",\"type\":\"int\"},"
                + "{\"name\":\"unit\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

        final Record record = decoder.decode("{\"id\":\"s\",\"reading\":1,\"unit\":\"C\"}".getBytes(UTF_8),
                SchemaInfo.builder().name("Sensor").type(SchemaType.JSON).schema(evolved.getBytes(UTF_8)).build());

        assertEquals("C", record.getValue("unit"));
    }

    /** The struct types map to a record directly; the primitives map to a single-field one (#189). */
    @Test
    public void supportsTheStructAndPrimitiveTypes() {
        assertTrue(TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.AVRO)));
        assertTrue(TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.JSON)));
        assertTrue("a primitive topic becomes a single-field record (#189)",
                TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.STRING)));
        assertFalse("KeyValue carries two schemas and needs its own handling (#190)",
                TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.KEY_VALUE)));
        assertFalse("a topic with no schema reports null, which is the #181 case",
                TopicSchemaRecordDecoder.supports(null));
    }

    /** Only the primitives need the reader-precedence rule, so struct types must not be flagged as such. */
    @Test
    public void onlyPrimitiveTopicsAreReportedAsPrimitive() {
        assertTrue(TopicSchemaRecordDecoder.isPrimitive(schemaInfo(SchemaType.STRING)));
        assertTrue(TopicSchemaRecordDecoder.isPrimitive(schemaInfo(SchemaType.INT32)));
        assertFalse(TopicSchemaRecordDecoder.isPrimitive(schemaInfo(SchemaType.AVRO)));
        assertFalse(TopicSchemaRecordDecoder.isPrimitive(schemaInfo(SchemaType.JSON)));
        assertFalse(TopicSchemaRecordDecoder.isPrimitive(null));
    }

    /** A payload that does not match the schema is an error the caller routes, not a half-built record. */
    @Test
    public void aPayloadThatDoesNotMatchTheSchemaFails() {
        assertThrows(IOException.class,
                () -> decoder.decode("not json at all".getBytes(UTF_8), schemaInfo(SchemaType.JSON)));
    }

    /** Nothing has been decoded yet, so there is no schema to report. */
    @Test
    public void theSchemaIsUnknownBeforeTheFirstMessage() {
        assertNull(new TopicSchemaRecordDecoder().getLastRecordSchema());
    }

    /**
     * Two threads decoding two schemas through one decoder must never see each other's shape (#195).
     * <p>
     * The cache was five mutable fields read and replaced one at a time, so a thread could pick up the
     * other's Avro schema together with its own RecordSchema. The failure was silent - no exception, wrong
     * records straight to success - and 13-36% of records were affected under contention. The decoder is
     * now created per batch rather than shared, but the class is also safe on its own, and this is what
     * says so.
     */
    @Test
    public void twoThreadsDecodingTwoSchemasDoNotSeeEachOther() throws Exception {
        final String alphaDef = "{\"type\":\"record\",\"name\":\"A\",\"fields\":["
                + "{\"name\":\"alpha\",\"type\":\"string\"}]}";
        final String betaDef = "{\"type\":\"record\",\"name\":\"B\",\"fields\":["
                + "{\"name\":\"beta\",\"type\":\"string\"}]}";

        final SchemaInfo alpha = SchemaInfo.builder().name("A").type(SchemaType.JSON)
                .schema(alphaDef.getBytes(UTF_8)).build();
        final SchemaInfo beta = SchemaInfo.builder().name("B").type(SchemaType.JSON)
                .schema(betaDef.getBytes(UTF_8)).build();

        final TopicSchemaRecordDecoder shared = new TopicSchemaRecordDecoder();
        final int iterations = 20_000;
        final java.util.concurrent.atomic.AtomicInteger wrong = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger failed = new java.util.concurrent.atomic.AtomicInteger();

        final Runnable alphaTask = decoding(shared, alpha, "alpha", "a-value", iterations, wrong, failed);
        final Runnable betaTask = decoding(shared, beta, "beta", "b-value", iterations, wrong, failed);

        final Thread one = new Thread(alphaTask);
        final Thread two = new Thread(betaTask);
        one.start();
        two.start();
        one.join();
        two.join();

        assertEquals("records came back with the other thread's shape or value", 0, wrong.get());
        assertEquals("decoding threw rather than returning a wrong record", 0, failed.get());
    }

    private static Runnable decoding(final TopicSchemaRecordDecoder decoder, final SchemaInfo schemaInfo,
            final String field, final String value, final int iterations,
            final java.util.concurrent.atomic.AtomicInteger wrong,
            final java.util.concurrent.atomic.AtomicInteger failed) {
        final byte[] payload = ("{\"" + field + "\":\"" + value + "\"}").getBytes(UTF_8);

        return () -> {
            for (int i = 0; i < iterations; i++) {
                try {
                    final Record record = decoder.decode(payload, schemaInfo);

                    if (!value.equals(record.getValue(field))
                            || !record.getSchema().getFieldNames().contains(field)) {
                        wrong.incrementAndGet();
                    }
                } catch (final Exception e) {
                    failed.incrementAndGet();
                }
            }
        };
    }

    // ------------------------------------------------------------------ helpers

    private static SchemaInfo schemaInfo(final SchemaType type) {
        return SchemaInfo.builder().name("Sensor").type(type).schema(SENSOR.getBytes(UTF_8)).build();
    }

    /** What an AVRO topic actually holds: bare binary, no file header and no embedded schema. */
    private static byte[] avroBytes(final String id, final int reading) throws Exception {
        final org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(SENSOR);
        final GenericData.Record record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("reading", reading);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final org.apache.avro.io.BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<org.apache.avro.generic.GenericRecord>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
