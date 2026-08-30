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

    /** Only the two struct types map to a record; everything else falls back to the Record Reader. */
    @Test
    public void supportsOnlyTheStructTypes() {
        assertTrue(TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.AVRO)));
        assertTrue(TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.JSON)));
        assertFalse("a primitive schema has no record shape (#189)",
                TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.STRING)));
        assertFalse("KeyValue carries two schemas and needs its own handling (#190)",
                TopicSchemaRecordDecoder.supports(schemaInfo(SchemaType.KEY_VALUE)));
        assertFalse("a topic with no schema reports null, which is the #181 case",
                TopicSchemaRecordDecoder.supports(null));
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
