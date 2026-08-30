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

import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * How many bytes precede the Avro body in a message (#207).
 * <p>
 * A schema may carry {@code __AVRO_READ_OFFSET__}, and Pulsar's own {@code GenericAvroReader} honours it:
 * it reads the property off the parsed Avro schema and starts its decoder there. The Kafka Connect adaptor
 * sets it to {@code 5} for Avro — the Confluent wire format's magic byte plus a four-byte schema id — so
 * every topic fed by a Debezium source carries a five-byte preamble ahead of the record.
 * <p>
 * Decoding from byte zero reads that preamble as record data. It rarely fails loudly: Avro's binary
 * encoding is permissive enough that the wrong bytes usually decode to a plausible-looking record, so the
 * symptom is wrong values rather than an exception.
 * <p>
 * The property is read from the <em>parsed Avro schema</em> first, which is where Pulsar looks and
 * therefore the authoritative source. {@link SchemaInfo#getProperties()} is consulted as a fallback, since
 * it costs nothing and a producer that records the offset there rather than in the schema document would
 * otherwise be silently mishandled. Absent, unparseable or negative all mean zero — today's behaviour, and
 * the default every topic that is not framed this way depends on.
 */
public final class AvroReadOffset {

    static final String PROPERTY = "__AVRO_READ_OFFSET__";

    private AvroReadOffset() {
    }

    /**
     * @param avroSchema the parsed schema, which is where Pulsar reads the property from
     * @param schemaInfo the topic's schema info, consulted only if the schema itself does not carry it
     * @return the number of bytes to skip, never negative
     */
    public static int of(final org.apache.avro.Schema avroSchema, final SchemaInfo schemaInfo) {
        Object property = avroSchema == null ? null : avroSchema.getObjectProp(PROPERTY);

        if (property == null && schemaInfo != null && schemaInfo.getProperties() != null) {
            property = schemaInfo.getProperties().get(PROPERTY);
        }

        if (property == null) {
            return 0;
        }

        try {
            final int offset = Integer.parseInt(property.toString().trim());
            return Math.max(offset, 0);
        } catch (final NumberFormatException e) {
            // A schema we cannot understand should not stop the topic being read the way it always was.
            return 0;
        }
    }

    /**
     * Checks the payload is long enough for the offset before decoding.
     *
     * @throws IOException if the message is shorter than its schema says the preamble is, which means the
     *                     message and the schema disagree - worth failing on rather than decoding whatever
     *                     the arithmetic happens to produce
     */
    public static void check(final int offset, final byte[] data) throws IOException {
        if (offset > data.length) {
            throw new IOException("The schema declares a read offset of " + offset + " bytes but the message "
                    + "is only " + data.length + " bytes long");
        }
    }
}
