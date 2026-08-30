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
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Topics whose registered schema is a single primitive value rather than a record (#189).
 * <p>
 * A primitive topic carries one value per message and has no fields, so there is no record shape to
 * derive - it has to be chosen. Each message becomes a record of exactly one field, named by the
 * processor's <em>Primitive Value Field</em> property, because NiFi's record model has no notion of a
 * bare scalar and every downstream writer needs a column name.
 * <p>
 * The wire formats are Pulsar's own: {@code Schema.STRING}, {@code Schema.INT32} and the rest already
 * encode and decode exactly what the broker validates against, so they are used directly rather than
 * reimplemented. That also means this stays correct if a format ever changes underneath us.
 * <p>
 * The date and time schemas ({@code DATE}, {@code TIME}, {@code TIMESTAMP}, {@code INSTANT},
 * {@code LOCAL_DATE}, {@code LOCAL_TIME}, {@code LOCAL_DATE_TIME}) are deliberately absent. They carry
 * conversion questions that the numeric types do not - which is the same reason logical types are still
 * uncovered for AVRO - and are better added with tests of their own.
 * <p>
 * {@code BYTES} is absent for a different and more important reason: it is not distinguishable from a
 * topic that has no schema at all. A producer created with {@code Schema.BYTES} registers nothing on the
 * topic, and {@code AUTO_PRODUCE_BYTES} reports a schema-less topic as {@code BYTES} with an empty
 * definition:
 * <pre>
 *     info={"name": "Bytes", "schema": "", "type": "BYTES"} type=BYTES
 * </pre>
 * Treating that as a primitive topic would capture every schema-less topic - which is what this bundle's
 * own publishers produce - and break the fallback to the Record Writer. A genuinely registered BYTES
 * schema is indistinguishable from none, so there is nothing to gain and a common path to lose.
 */
public final class PrimitiveTopicSchema {

    private static final Map<SchemaType, DataType> TYPES;

    static {
        final Map<SchemaType, DataType> types = new EnumMap<>(SchemaType.class);
        types.put(SchemaType.STRING, RecordFieldType.STRING.getDataType());
        types.put(SchemaType.BOOLEAN, RecordFieldType.BOOLEAN.getDataType());
        types.put(SchemaType.INT8, RecordFieldType.BYTE.getDataType());
        types.put(SchemaType.INT16, RecordFieldType.SHORT.getDataType());
        types.put(SchemaType.INT32, RecordFieldType.INT.getDataType());
        types.put(SchemaType.INT64, RecordFieldType.LONG.getDataType());
        types.put(SchemaType.FLOAT, RecordFieldType.FLOAT.getDataType());
        types.put(SchemaType.DOUBLE, RecordFieldType.DOUBLE.getDataType());
        TYPES = Collections.unmodifiableMap(types);
    }

    private PrimitiveTopicSchema() {
    }

    /**
     * A scalar rendered as text. Structured values are refused rather than coerced: {@code toString()} on a
     * record or a collection produces a Java debug string, and publishing that to a STRING topic would look
     * like it worked while putting something like {@code MapRecord[{id=1}]} on the topic.
     */
    private static String asString(final Object value, final String fieldName) throws IOException {
        if (value instanceof org.apache.nifi.serialization.record.Record
                || value instanceof java.util.Map || value instanceof Object[]
                || value instanceof Iterable) {
            throw new IOException("Field '" + fieldName + "' is a " + value.getClass().getSimpleName()
                    + ", which has no meaningful text form for a STRING topic; publish a scalar field or "
                    + "use a topic whose schema is a record");
        }

        return DataTypeUtils.toString(value, (String) null);
    }

    /** Whether a topic of this schema type can be mapped to a single-field record. */
    public static boolean supports(final SchemaType type) {
        return type != null && TYPES.containsKey(type);
    }

    /** The NiFi type the single field takes, so writers downstream see the topic's actual type. */
    public static DataType dataTypeOf(final SchemaType type) {
        return TYPES.get(type);
    }

    /**
     * Decodes one message into the value of the single field.
     *
     * @param type the topic's schema type, which must {@link #supports}
     * @param data the raw message payload
     * @return the decoded value
     * @throws IOException if the payload is not valid for the schema
     */
    public static Object decode(final SchemaType type, final byte[] data) throws IOException {
        try {
            switch (type) {
                case STRING:  return Schema.STRING.decode(data);
                case BOOLEAN: return Schema.BOOL.decode(data);
                case INT8:    return Schema.INT8.decode(data);
                case INT16:   return Schema.INT16.decode(data);
                case INT32:   return Schema.INT32.decode(data);
                case INT64:   return Schema.INT64.decode(data);
                case FLOAT:   return Schema.FLOAT.decode(data);
                case DOUBLE:  return Schema.DOUBLE.decode(data);
                default:      throw new IOException("Unsupported primitive schema type " + type);
            }
        } catch (final RuntimeException e) {
            // A payload of the wrong width throws from the client's own decoder; the caller routes it to
            // parse.failure rather than letting a malformed value reach a record set.
            throw new IOException("The message is not a valid " + type + " value", e);
        }
    }

    /**
     * Encodes a field value with the topic's primitive schema. The value is coerced to the schema's type
     * first, so a record whose field is a String on a topic of INT32 publishes the number rather than
     * failing at the broker with a validation error that says nothing about which record was at fault.
     *
     * @param type the topic's schema type, which must {@link #supports}
     * @param value the value of the record's single field, which may be null
     * @param fieldName the field's name, for the error message when coercion fails
     * @return the encoded payload
     * @throws IOException if the value cannot be represented in the topic's schema
     */
    public static byte[] encode(final SchemaType type, final Object value, final String fieldName)
            throws IOException {
        if (value == null) {
            throw new IOException("Field '" + fieldName + "' is null, which a " + type + " topic cannot carry");
        }

        try {
            switch (type) {
                case STRING:  return Schema.STRING.encode(asString(value, fieldName));
                case BOOLEAN: return Schema.BOOL.encode(DataTypeUtils.toBoolean(value, fieldName));
                case INT8:    return Schema.INT8.encode(DataTypeUtils.toByte(value, fieldName));
                case INT16:   return Schema.INT16.encode(DataTypeUtils.toShort(value, fieldName));
                case INT32:   return Schema.INT32.encode(DataTypeUtils.toInteger(value, fieldName));
                case INT64:   return Schema.INT64.encode(DataTypeUtils.toLong(value, fieldName));
                case FLOAT:   return Schema.FLOAT.encode(DataTypeUtils.toFloat(value, fieldName));
                case DOUBLE:  return Schema.DOUBLE.encode(DataTypeUtils.toDouble(value, fieldName));
                default:      throw new IOException("Unsupported primitive schema type " + type);
            }
        } catch (final RuntimeException e) {
            // IllegalTypeConversionException for a type that cannot convert, NumberFormatException for a
            // String that is not a number. Both have to become an IOException here: an unchecked exception
            // escaping the publish loop would bypass the failure routing and fail the whole trigger.
            throw new IOException("Field '" + fieldName + "' cannot be published to a " + type + " topic", e);
        }
    }

}
