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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Topics whose schema carries a key and a value, each with a schema of its own (#190).
 * <p>
 * A {@code KEY_VALUE} topic is two schemas and an encoding that says where the key is written:
 * <ul>
 *   <li>{@code INLINE} - key and value are both in the payload, length-prefixed.</li>
 *   <li>{@code SEPARATED} - the key is in the message's key metadata and only the value is in the
 *       payload. This is what makes a topic compactable by key, and is the more common of the two.</li>
 * </ul>
 * Each message becomes a record with two fields, {@code key} and {@code value}, named by the processor's
 * properties. Each side is typed by its own schema rather than forced into a nested record: a
 * {@code STRING} key becomes a string field and an {@code AVRO} value becomes a nested record, so the
 * shape downstream is the shape the topic actually describes.
 * <p>
 * The length-prefixed {@code INLINE} format is Pulsar's, so {@link KeyValue#encode} and
 * {@link KeyValue#decode} are used rather than reimplementing it.
 */
public class KeyValueTopicSchema {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Defaults for the field names, and the defaults of the processor properties. */
    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    private String cachedDefinition;
    private String cachedKeyField;
    private String cachedValueField;
    private RecordSchema cachedRecordSchema;
    private SchemaInfo cachedKeySchema;
    private SchemaInfo cachedValueSchema;
    private KeyValueEncodingType cachedEncoding;

    /**
     * The parsed side schemas, cached alongside the definition they came from. These were re-derived on
     * every message - {@code Schema.Parser().parse} plus {@code AvroTypeUtil.createSchema} per side, and
     * the Avro schema parsed a second time for the datum reader - while the primitive and top-level struct
     * paths both cached. Parsing is the expensive part of decoding a small message.
     */
    private DataType cachedKeyType;
    private DataType cachedValueType;
    private org.apache.avro.Schema cachedKeyAvro;
    private org.apache.avro.Schema cachedValueAvro;

    /** Whether this topic carries a key schema and a value schema. */
    public static boolean supports(final SchemaInfo schemaInfo) {
        return schemaInfo != null && schemaInfo.getType() == SchemaType.KEY_VALUE;
    }

    /**
     * Decodes one message into a record of its key and its value.
     *
     * @param payload the message payload, which holds both sides when the encoding is INLINE
     * @param messageKey the message's key bytes, used when the encoding is SEPARATED; may be null
     * @param schemaInfo the topic's KeyValue schema
     * @param keyField the name to give the key field
     * @param valueField the name to give the value field
     * @return a record of the two
     * @throws IOException if either side does not match its schema
     */
    public Record decode(final byte[] payload, final byte[] messageKey, final SchemaInfo schemaInfo,
            final String keyField, final String valueField) throws IOException {
        parse(schemaInfo, keyField, valueField);

        final byte[] keyBytes;
        final byte[] valueBytes;

        if (cachedEncoding == KeyValueEncodingType.SEPARATED) {
            if (messageKey == null) {
                throw new IOException("The topic's schema is SEPARATED, so the key is carried in the "
                        + "message's key metadata, but this message has none");
            }
            keyBytes = messageKey;
            valueBytes = payload;
        } else {
            final KeyValue<byte[], byte[]> split = splitInline(payload);
            keyBytes = split.getKey();
            valueBytes = split.getValue();
        }

        final Map<String, Object> values = new HashMap<>(2);
        values.put(keyField, decodeSide(keyBytes, cachedKeySchema, cachedKeyType, cachedKeyAvro));
        values.put(valueField, decodeSide(valueBytes, cachedValueSchema, cachedValueType, cachedValueAvro));

        return new MapRecord(cachedRecordSchema, values, false, true);
    }

    /**
     * Encodes a record's key and value fields for this topic.
     *
     * @return the payload, and the message key when the encoding is SEPARATED; the key is null otherwise
     * @throws IOException if the record has no key or value field, or either side cannot be encoded
     */
    public EncodedKeyValue encode(final Record record, final SchemaInfo schemaInfo, final String keyField,
            final String valueField) throws IOException {
        parse(schemaInfo, keyField, valueField);

        final RecordSchema recordSchema = record.getSchema();

        if (!recordSchema.getField(keyField).isPresent() || !recordSchema.getField(valueField).isPresent()) {
            throw new IOException("A KeyValue topic needs a record with '" + keyField + "' and '" + valueField
                    + "' fields, but the record has " + recordSchema.getFieldNames());
        }

        final byte[] keyBytes = encodeSide(record.getValue(keyField), cachedKeySchema, keyField);
        final byte[] valueBytes = encodeSide(record.getValue(valueField), cachedValueSchema, valueField);

        if (cachedEncoding == KeyValueEncodingType.SEPARATED) {
            return new EncodedKeyValue(valueBytes, keyBytes);
        }

        return new EncodedKeyValue(KeyValue.encode(keyBytes, org.apache.pulsar.client.api.Schema.BYTES,
                valueBytes, org.apache.pulsar.client.api.Schema.BYTES), null);
    }

    /** A payload, and the message key that must accompany it on a SEPARATED topic. */
    public static final class EncodedKeyValue {

        private final byte[] payload;
        private final byte[] messageKey;

        EncodedKeyValue(final byte[] payload, final byte[] messageKey) {
            this.payload = payload;
            this.messageKey = messageKey;
        }

        public byte[] getPayload() {
            return payload;
        }

        /** Null unless the topic's encoding is SEPARATED, in which case it must be set on the message. */
        public byte[] getMessageKey() {
            return messageKey;
        }
    }

    // ------------------------------------------------------------------ internals

    /** Parsed once and re-parsed only when the definition or the field names change. */
    private void parse(final SchemaInfo schemaInfo, final String keyField, final String valueField)
            throws IllegalStateException {
        final String definition = new String(schemaInfo.getSchema(), StandardCharsets.UTF_8);

        if (cachedRecordSchema != null && definition.equals(cachedDefinition)
                && java.util.Objects.equals(keyField, cachedKeyField)
                && java.util.Objects.equals(valueField, cachedValueField)) {
            return;
        }

        final KeyValue<SchemaInfo, SchemaInfo> schemas = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
        cachedKeySchema = schemas.getKey();
        cachedValueSchema = schemas.getValue();
        cachedEncoding = KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo);

        cachedKeyAvro = avroSchemaOf(cachedKeySchema);
        cachedValueAvro = avroSchemaOf(cachedValueSchema);
        cachedKeyType = dataTypeOf(cachedKeySchema, cachedKeyAvro);
        cachedValueType = dataTypeOf(cachedValueSchema, cachedValueAvro);

        final List<RecordField> fields = new ArrayList<>(2);
        fields.add(new RecordField(keyField, cachedKeyType));
        fields.add(new RecordField(valueField, cachedValueType));

        cachedRecordSchema = new SimpleRecordSchema(fields);
        cachedDefinition = definition;
        cachedKeyField = keyField;
        cachedValueField = valueField;
    }

    /** Each side keeps the shape its own schema describes: a scalar stays a scalar, a record a record. */
    private static org.apache.avro.Schema avroSchemaOf(final SchemaInfo side) {
        if (side.getType() == SchemaType.AVRO || side.getType() == SchemaType.JSON) {
            return new org.apache.avro.Schema.Parser().parse(new String(side.getSchema(), StandardCharsets.UTF_8));
        }

        return null;
    }

    private static DataType dataTypeOf(final SchemaInfo side, final org.apache.avro.Schema avroSchema) {
        if (PrimitiveTopicSchema.supports(side.getType())) {
            return PrimitiveTopicSchema.dataTypeOf(side.getType());
        }

        if (avroSchema != null) {
            return RecordFieldType.RECORD.getRecordDataType(AvroTypeUtil.createSchema(avroSchema));
        }

        // BYTES is how a side with no schema of its own is reported, and anything else is a type we do
        // not decode; either way the raw bytes are the honest answer rather than a guess at a shape.
        return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
    }

    private static Object decodeSide(final byte[] data, final SchemaInfo side, final DataType dataType,
            final org.apache.avro.Schema avroSchema) throws IOException {
        if (PrimitiveTopicSchema.supports(side.getType())) {
            return PrimitiveTopicSchema.decode(side.getType(), data);
        }

        if (avroSchema != null) {
            final RecordSchema schema =
                    ((org.apache.nifi.serialization.record.type.RecordDataType) dataType).getChildSchema();

            if (side.getType() == SchemaType.JSON) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> values = JSON.readValue(data, Map.class);
                // Through DataTypeUtils rather than a MapRecord constructor: neither the checked nor the
                // unchecked constructor converts a nested JSON object, so a nested field came back as a
                // raw LinkedHashMap that no downstream writer expecting a record could handle.
                return org.apache.nifi.serialization.record.util.DataTypeUtils.toRecord(values, schema, null);
            }

            final org.apache.avro.generic.GenericRecord avroRecord =
                    new org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericRecord>(avroSchema)
                            .read(null, org.apache.avro.io.DecoderFactory.get().binaryDecoder(data, null));

            return new MapRecord(schema, AvroTypeUtil.convertAvroRecordToMap(avroRecord, schema));
        }

        return data;
    }

    private static byte[] encodeSide(final Object value, final SchemaInfo side, final String fieldName)
            throws IOException {
        if (PrimitiveTopicSchema.supports(side.getType())) {
            return PrimitiveTopicSchema.encode(side.getType(), value, fieldName);
        }

        if (side.getType() == SchemaType.AVRO || side.getType() == SchemaType.JSON) {
            if (!(value instanceof Record)) {
                throw new IOException("Field '" + fieldName + "' must be a record to match the topic's "
                        + side.getType() + " schema, but is "
                        + (value == null ? "null" : value.getClass().getSimpleName()));
            }
            return TopicSchemaRecordDecoder.encodeStruct((Record) value, side);
        }

        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        throw new IOException("Field '" + fieldName + "' cannot be encoded for a " + side.getType() + " schema");
    }

    /** The INLINE layout is Pulsar's, so its own decoder splits it rather than our arithmetic. */
    private static KeyValue<byte[], byte[]> splitInline(final byte[] payload) throws IOException {
        try {
            return KeyValue.decode(payload, (keyBytes, valueBytes) -> new KeyValue<>(keyBytes, valueBytes));
        } catch (final RuntimeException e) {
            throw new IOException("The message is not a valid INLINE KeyValue payload; it was "
                    + payload.length + " bytes: " + Arrays.toString(Arrays.copyOf(payload, Math.min(8, payload.length))), e);
        }
    }
}
