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
import java.util.Map;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.avro.AvroTypeUtil;
import java.util.Collections;
import java.util.HashMap;

import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Builds NiFi records from the schema a Pulsar topic carries, rather than from a configured Record Reader.
 * <p>
 * The field definitions come from the broker, not from the payload and not from message properties: Pulsar
 * keeps schemas in a registry keyed by topic and version, and {@code Schema.AUTO_CONSUME()} attaches the
 * schema of each message's version as its reader schema. {@link SchemaInfo#getSchema()} is that definition,
 * and for both {@code AVRO} and {@code JSON} topics it is an Avro schema document - only the
 * {@link SchemaType} says how the payload itself is encoded. Because the definition arrives per message,
 * schema evolution needs no special handling here: a message published under an older version decodes with
 * the version it was written with.
 * <p>
 * This is the mirror of {@code PublisherLease}'s encoding, and deliberately decodes the bytes itself rather
 * than using the value Pulsar already decoded. The Pulsar client shades Avro, so the record behind
 * {@code msg.getValue()} is a {@code org.apache.pulsar.shade.org.apache.avro} type that NiFi's
 * {@link AvroTypeUtil} cannot accept. Parsing the definition with unshaded Avro keeps both directions on
 * the same conversion code.
 */
public class TopicSchemaRecordDecoder {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Parsed once and re-parsed only when the definition itself changes, as leases do on the publish side. */
    private String cachedDefinition;
    private SchemaType cachedType;
    private org.apache.avro.Schema cachedAvroSchema;
    private RecordSchema cachedRecordSchema;
    private String cachedPrimitiveField;

    /** Used when a caller does not name the field, and the default of the processor property. */
    public static final String DEFAULT_PRIMITIVE_FIELD = "value";

    /**
     * Whether records can be built from this schema. Only the two struct types Pulsar registers as an Avro
     * document are supported; a topic with no schema reports {@code null} here, and a primitive or
     * {@code KEY_VALUE} schema has no record shape to map, so both fall back to the Record Reader.
     */
    public static boolean supports(final SchemaInfo schemaInfo) {
        return schemaInfo != null
                && (schemaInfo.getType() == SchemaType.AVRO || schemaInfo.getType() == SchemaType.JSON
                        || PrimitiveTopicSchema.supports(schemaInfo.getType()));
    }

    /**
     * Whether this topic's schema is a single primitive value rather than a record (#189). Callers treat
     * these differently: a primitive payload is often something a Record Reader should parse - JSON text on
     * a STRING topic is a common shape - so a configured reader takes precedence over wrapping the value.
     */
    public static boolean isPrimitive(final SchemaInfo schemaInfo) {
        return schemaInfo != null && PrimitiveTopicSchema.supports(schemaInfo.getType());
    }

    /**
     * Decodes one message into a record shaped by the topic's schema.
     *
     * @param data the raw message payload
     * @param schemaInfo the schema the message was published under, which must {@link #supports} it
     * @return the decoded record
     * @throws IOException if the payload does not match the schema
     */
    public Record decode(final byte[] data, final SchemaInfo schemaInfo) throws IOException {
        return decode(data, schemaInfo, DEFAULT_PRIMITIVE_FIELD);
    }

    /**
     * Decodes one message into a record shaped by the topic's schema.
     *
     * @param data the raw message payload
     * @param schemaInfo the schema the message was published under, which must {@link #supports} it
     * @param primitiveField the field name to give the value of a primitive topic
     * @return the decoded record
     * @throws IOException if the payload does not match the schema
     */
    public Record decode(final byte[] data, final SchemaInfo schemaInfo, final String primitiveField)
            throws IOException {
        if (PrimitiveTopicSchema.supports(schemaInfo.getType())) {
            return decodePrimitive(data, schemaInfo.getType(), primitiveField);
        }

        final String definition = new String(schemaInfo.getSchema(), StandardCharsets.UTF_8);

        if (cachedRecordSchema == null || !definition.equals(cachedDefinition) || cachedType != schemaInfo.getType()) {
            cachedAvroSchema = new org.apache.avro.Schema.Parser().parse(definition);
            cachedRecordSchema = AvroTypeUtil.createSchema(cachedAvroSchema);
            cachedDefinition = definition;
            cachedType = schemaInfo.getType();
        }

        return schemaInfo.getType() == SchemaType.AVRO ? decodeAvro(data) : decodeJson(data);
    }

    /** AVRO topics carry bare Avro binary - no file header, no embedded schema - so the schema comes from us. */
    private Record decodeAvro(final byte[] data) throws IOException {
        final GenericDatumReader<org.apache.avro.generic.GenericRecord> datumReader =
                new GenericDatumReader<>(cachedAvroSchema);

        final org.apache.avro.generic.GenericRecord avroRecord =
                datumReader.read(null, DecoderFactory.get().binaryDecoder(data, null));

        return new MapRecord(cachedRecordSchema, AvroTypeUtil.convertAvroRecordToMap(avroRecord, cachedRecordSchema));
    }

    /**
     * JSON topics carry plain JSON text - the shape Pulsar's own JSON schema writes, not Avro's JSON
     * encoding - so it is parsed as JSON and coerced to the schema's types. Coercion matters because JSON
     * has no way to distinguish an int from a long, or a string from a UUID.
     */
    @SuppressWarnings("unchecked")
    private Record decodeJson(final byte[] data) throws IOException {
        final Map<String, Object> values = JSON.readValue(data, Map.class);

        return new MapRecord(cachedRecordSchema, values, false, true);
    }

    /**
     * A primitive topic has one value per message and no fields, so the record shape is chosen rather than
     * derived: a single field, named by the caller, typed as the topic's schema.
     */
    private Record decodePrimitive(final byte[] data, final SchemaType type, final String fieldName)
            throws IOException {
        if (cachedRecordSchema == null || cachedType != type || !fieldName.equals(cachedPrimitiveField)) {
            cachedRecordSchema = new SimpleRecordSchema(Collections.singletonList(
                    new RecordField(fieldName, PrimitiveTopicSchema.dataTypeOf(type))));
            cachedAvroSchema = null;
            cachedDefinition = null;
            cachedType = type;
            cachedPrimitiveField = fieldName;
        }

        final Map<String, Object> values = new HashMap<>(1);
        values.put(fieldName, PrimitiveTopicSchema.decode(type, data));

        return new MapRecord(cachedRecordSchema, values, false, true);
    }

    /** The schema the last decoded message was shaped by, for callers that group records into sets. */
    public RecordSchema getLastRecordSchema() {
        return cachedRecordSchema;
    }
}
