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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

/**
 * Encoding and decoding the single value a primitive topic carries (#189).
 * <p>
 * Every encode is checked against Pulsar's own decoder rather than against our own encoder, so a test
 * cannot pass by being wrong in both directions - which is the failure mode a hand-rolled wire format
 * would have.
 */
public class PrimitiveTopicSchemaTest {

    @Test
    public void valuesRoundTripThroughPulsarsOwnCodecs() throws Exception {
        assertEquals("hello", Schema.STRING.decode(PrimitiveTopicSchema.encode(SchemaType.STRING, "hello", "v")));
        assertEquals(Boolean.TRUE, Schema.BOOL.decode(PrimitiveTopicSchema.encode(SchemaType.BOOLEAN, true, "v")));
        assertEquals(Byte.valueOf((byte) 7), Schema.INT8.decode(PrimitiveTopicSchema.encode(SchemaType.INT8, (byte) 7, "v")));
        assertEquals(Short.valueOf((short) 12345), Schema.INT16.decode(PrimitiveTopicSchema.encode(SchemaType.INT16, (short) 12345, "v")));
        assertEquals(Integer.valueOf(42), Schema.INT32.decode(PrimitiveTopicSchema.encode(SchemaType.INT32, 42, "v")));
        assertEquals(Long.valueOf(9000000000L), Schema.INT64.decode(PrimitiveTopicSchema.encode(SchemaType.INT64, 9000000000L, "v")));
        assertEquals(Float.valueOf(1.5f), Schema.FLOAT.decode(PrimitiveTopicSchema.encode(SchemaType.FLOAT, 1.5f, "v")));
        assertEquals(Double.valueOf(2.5d), Schema.DOUBLE.decode(PrimitiveTopicSchema.encode(SchemaType.DOUBLE, 2.5d, "v")));
    }

    @Test
    public void decodingReadsWhatPulsarWrote() throws Exception {
        assertEquals("hello", PrimitiveTopicSchema.decode(SchemaType.STRING, Schema.STRING.encode("hello")));
        assertEquals(42, PrimitiveTopicSchema.decode(SchemaType.INT32, Schema.INT32.encode(42)));
        assertEquals(9000000000L, PrimitiveTopicSchema.decode(SchemaType.INT64, Schema.INT64.encode(9000000000L)));
        assertEquals(true, PrimitiveTopicSchema.decode(SchemaType.BOOLEAN, Schema.BOOL.encode(true)));
    }

    /**
     * A record field's type need not match the topic's. Coercing here means a CSV reader's String "42" can
     * be published to an INT32 topic, rather than failing at the broker with an error that names neither
     * the field nor the record.
     */
    @Test
    public void valuesAreCoercedToTheTopicsType() throws Exception {
        assertEquals(Integer.valueOf(42), Schema.INT32.decode(PrimitiveTopicSchema.encode(SchemaType.INT32, "42", "v")));
        assertEquals("42", Schema.STRING.decode(PrimitiveTopicSchema.encode(SchemaType.STRING, 42, "v")));
        assertEquals(Long.valueOf(42L), Schema.INT64.decode(PrimitiveTopicSchema.encode(SchemaType.INT64, 42, "v")));
    }

    /** A value that cannot be represented is an error the caller routes, not a silently mangled message. */
    @Test
    public void aValueThatCannotBeRepresentedFails() {
        assertThrows(IOException.class, () -> PrimitiveTopicSchema.encode(SchemaType.INT32, "not a number", "reading"));
        assertThrows("null has no representation on a primitive topic", IOException.class,
                () -> PrimitiveTopicSchema.encode(SchemaType.INT32, null, "reading"));
    }

    /**
     * A structured value has no meaningful text form, so it is refused rather than published as its
     * toString(). Coercing it would put a Java debug string on the topic and look like it had worked.
     */
    @Test
    public void aStructuredValueIsNotStringifiedOntoAStringTopic() {
        final java.util.Map<String, Object> nested = new java.util.HashMap<>();
        nested.put("id", 1);

        assertThrows(IOException.class, () -> PrimitiveTopicSchema.encode(SchemaType.STRING, nested, "payload"));
        assertThrows(IOException.class,
                () -> PrimitiveTopicSchema.encode(SchemaType.STRING, new Object[] {1, 2}, "payload"));
    }

    /** Scalars still render as text, including numbers and booleans. */
    @Test
    public void scalarsStillRenderAsText() throws Exception {
        assertEquals("42", Schema.STRING.decode(PrimitiveTopicSchema.encode(SchemaType.STRING, 42, "v")));
        assertEquals("true", Schema.STRING.decode(PrimitiveTopicSchema.encode(SchemaType.STRING, true, "v")));
    }

    /** A payload of the wrong width is a parse failure, not a garbage value. */
    @Test
    public void aPayloadOfTheWrongWidthFails() {
        assertThrows(IOException.class, () -> PrimitiveTopicSchema.decode(SchemaType.INT32, new byte[] {1, 2}));
    }

    @Test
    public void theDateAndTimeSchemasAreNotSupportedYet() {
        assertTrue(PrimitiveTopicSchema.supports(SchemaType.STRING));
        assertTrue(PrimitiveTopicSchema.supports(SchemaType.INT32));
        assertFalse("BYTES is how the client reports a topic with no schema at all, so treating it as a "
                + "primitive would capture every schema-less topic",
                PrimitiveTopicSchema.supports(SchemaType.BYTES));
        assertFalse("deliberately deferred, along with AVRO logical types",
                PrimitiveTopicSchema.supports(SchemaType.TIMESTAMP));
        assertFalse(PrimitiveTopicSchema.supports(SchemaType.LOCAL_DATE_TIME));
        assertFalse("a record type is not a primitive", PrimitiveTopicSchema.supports(SchemaType.AVRO));
        assertFalse(PrimitiveTopicSchema.supports(null));
    }
}
