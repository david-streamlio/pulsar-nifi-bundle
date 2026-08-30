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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.junit.Test;

/**
 * Encoding and decoding both sides of a KeyValue topic, without a broker.
 * <p>
 * The INLINE round trips go through Pulsar's own {@link org.apache.pulsar.common.schema.KeyValue#encode}
 * and {@code decode}, so a test cannot pass by being wrong in both directions.
 */
public class KeyValueTopicSchemaTest {

    private static final String READING = "{\"type\":\"record\",\"name\":\"Reading\",\"fields\":["
            + "{\"name\":\"reading\",\"type\":\"int\"}]}";

    /** A schema with a nested record, which is the shape that broke the JSON encoding. */
    private static final String ORDER = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"int\"},"
            + "{\"name\":\"customer\",\"type\":{\"type\":\"record\",\"name\":\"Customer\",\"fields\":["
            + "{\"name\":\"name\",\"type\":\"string\"}]}}]}";

    private final KeyValueTopicSchema schema = new KeyValueTopicSchema();

    @Test
    public void aSeparatedTopicTakesItsKeyFromTheMessageMetadata() throws Exception {
        final SchemaInfo info = keyValueSchema(READING, org.apache.pulsar.common.schema.SchemaType.AVRO,
                KeyValueEncodingType.SEPARATED);

        final Record record = schema.decode(avro(READING, "reading", 42), Schema.STRING.encode("device-1"),
                info, "key", "value");

        assertEquals("device-1", record.getValue("key"));
        assertEquals(42, ((Record) record.getValue("value")).getValue("reading"));
    }

    @Test
    public void anInlineTopicTakesBothSidesFromThePayload() throws Exception {
        final SchemaInfo info = keyValueSchema(READING, org.apache.pulsar.common.schema.SchemaType.AVRO,
                KeyValueEncodingType.INLINE);

        final byte[] payload = org.apache.pulsar.common.schema.KeyValue.encode(
                Schema.STRING.encode("device-2"), Schema.BYTES, avro(READING, "reading", 43), Schema.BYTES);

        final Record record = schema.decode(payload, null, info, "key", "value");

        assertEquals("device-2", record.getValue("key"));
        assertEquals(43, ((Record) record.getValue("value")).getValue("reading"));
    }

    /** A SEPARATED topic without a message key has nowhere for its key to have come from. */
    @Test
    public void aSeparatedMessageWithoutAKeyIsAnError() {
        final SchemaInfo info = keyValueSchema(READING, org.apache.pulsar.common.schema.SchemaType.AVRO,
                KeyValueEncodingType.SEPARATED);

        assertThrows(IOException.class, () -> schema.decode(avro(READING, "reading", 1), null, info, "key", "value"));
    }

    /** The encoded key goes back on the message on a SEPARATED topic, and the payload holds only the value. */
    @Test
    public void encodingSeparatesTheKeyFromThePayload() throws Exception {
        final SchemaInfo info = keyValueSchema(READING, org.apache.pulsar.common.schema.SchemaType.AVRO,
                KeyValueEncodingType.SEPARATED);

        final KeyValueTopicSchema.EncodedKeyValue encoded =
                schema.encode(readingRecord("device-3", 44), info, "key", "value");

        assertNotNull("a SEPARATED topic needs the key on the message", encoded.getMessageKey());
        assertEquals("device-3", Schema.STRING.decode(encoded.getMessageKey()));
        assertEquals("the payload holds the value alone", 44,
                ((Record) schema.decode(encoded.getPayload(), encoded.getMessageKey(), info, "key", "value")
                        .getValue("value")).getValue("reading"));
    }

    /** INLINE puts both sides in the payload and leaves the message key free. */
    @Test
    public void encodingInlineLeavesTheMessageKeyFree() throws Exception {
        final SchemaInfo info = keyValueSchema(READING, org.apache.pulsar.common.schema.SchemaType.AVRO,
                KeyValueEncodingType.INLINE);

        final KeyValueTopicSchema.EncodedKeyValue encoded =
                schema.encode(readingRecord("device-4", 45), info, "key", "value");

        assertNull("INLINE must not claim the message key", encoded.getMessageKey());

        final Record decoded = schema.decode(encoded.getPayload(), null, info, "key", "value");
        assertEquals("device-4", decoded.getValue("key"));
        assertEquals(45, ((Record) decoded.getValue("value")).getValue("reading"));
    }

    /**
     * The finding from review: a JSON side with a nested record could not be published. Building a map of
     * the record's values handed Jackson a MapRecord, which it cannot serialize, so every such FlowFile
     * went to failure. The flat value schema used by the integration tests could not see it.
     */
    @Test
    public void aJsonSideWithANestedRecordEncodes() throws Exception {
        final SchemaInfo info = keyValueSchema(ORDER, org.apache.pulsar.common.schema.SchemaType.JSON,
                KeyValueEncodingType.INLINE);

        final RecordSchema customer = new SimpleRecordSchema(Arrays.asList(
                new RecordField("name", RecordFieldType.STRING.getDataType())));
        final Map<String, Object> customerValues = new HashMap<>();
        customerValues.put("name", "Ada");

        final RecordSchema order = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("customer", RecordFieldType.RECORD.getRecordDataType(customer))));
        final Map<String, Object> orderValues = new HashMap<>();
        orderValues.put("id", 7);
        orderValues.put("customer", new MapRecord(customer, customerValues));

        final Record record = record("device-5", new MapRecord(order, orderValues), order);
        final KeyValueTopicSchema.EncodedKeyValue encoded = schema.encode(record, info, "key", "value");

        final Record decoded = schema.decode(encoded.getPayload(), null, info, "key", "value");
        final Record value = (Record) decoded.getValue("value");

        assertEquals(7, value.getValue("id"));
        assertEquals("the nested record must survive", "Ada",
                ((Record) value.getValue("customer")).getValue("name"));
    }

    /** A record missing either side cannot be mapped onto the topic's two schemas. */
    @Test
    public void aRecordMissingASideIsRefused() {
        final SchemaInfo info = keyValueSchema(READING, org.apache.pulsar.common.schema.SchemaType.AVRO,
                KeyValueEncodingType.SEPARATED);

        final RecordSchema keyOnly = new SimpleRecordSchema(Arrays.asList(
                new RecordField("key", RecordFieldType.STRING.getDataType())));
        final Map<String, Object> values = new HashMap<>();
        values.put("key", "device-6");

        final IOException thrown = assertThrows(IOException.class,
                () -> schema.encode(new MapRecord(keyOnly, values), info, "key", "value"));
        assertTrue("the message should name the fields the record does have",
                thrown.getMessage().contains("key"));
    }

    /** A payload that is not a valid INLINE frame is a decode error, not a silently truncated record. */
    @Test
    public void aMalformedInlinePayloadIsAnError() {
        final SchemaInfo info = keyValueSchema(READING, org.apache.pulsar.common.schema.SchemaType.AVRO,
                KeyValueEncodingType.INLINE);

        assertThrows(IOException.class, () -> schema.decode(new byte[] {1, 2, 3}, null, info, "key", "value"));
    }

    @Test
    public void onlyKeyValueTopicsAreSupported() {
        assertTrue(KeyValueTopicSchema.supports(keyValueSchema(READING,
                org.apache.pulsar.common.schema.SchemaType.AVRO, KeyValueEncodingType.SEPARATED)));
        assertTrue(!KeyValueTopicSchema.supports(SchemaInfo.builder().name("R")
                .type(org.apache.pulsar.common.schema.SchemaType.AVRO).schema(READING.getBytes(UTF_8)).build()));
        assertTrue(!KeyValueTopicSchema.supports(null));
    }

    // ------------------------------------------------------------------ helpers

    private static SchemaInfo keyValueSchema(final String valueDefinition,
            final org.apache.pulsar.common.schema.SchemaType valueType, final KeyValueEncodingType encoding) {
        final SchemaInfo value = SchemaInfo.builder().name("V").type(valueType)
                .schema(valueDefinition.getBytes(UTF_8)).build();

        return KeyValueSchemaInfo.encodeKeyValueSchemaInfo("kv", Schema.STRING.getSchemaInfo(), value, encoding);
    }

    private static byte[] avro(final String definition, final String field, final int value) throws Exception {
        final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(definition);
        final org.apache.avro.generic.GenericData.Record record =
                new org.apache.avro.generic.GenericData.Record(avroSchema);
        record.put(field, value);

        final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        final org.apache.avro.io.BinaryEncoder encoder = org.apache.avro.io.EncoderFactory.get()
                .binaryEncoder(out, null);
        new org.apache.avro.generic.GenericDatumWriter<org.apache.avro.generic.GenericRecord>(avroSchema)
                .write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static Record readingRecord(final String key, final int reading) {
        final RecordSchema value = new SimpleRecordSchema(Arrays.asList(
                new RecordField("reading", RecordFieldType.INT.getDataType())));
        final Map<String, Object> values = new HashMap<>();
        values.put("reading", reading);

        return record(key, new MapRecord(value, values), value);
    }

    private static Record record(final String key, final Record value, final RecordSchema valueSchema) {
        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("key", RecordFieldType.STRING.getDataType()),
                new RecordField("value", RecordFieldType.RECORD.getRecordDataType(valueSchema))));

        final Map<String, Object> values = new HashMap<>();
        values.put("key", key);
        values.put("value", value);

        return new MapRecord(schema, values);
    }
}
