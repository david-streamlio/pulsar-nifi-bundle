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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collections;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

/** The schema-info-only lookup the Record Reader path uses (#207). */
public class AvroReadOffsetTest {

    private static final String FIELDS = "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]";

    @Test
    public void readsTheOffsetFromTheSchemaDefinition() {
        assertEquals(5, AvroReadOffset.of(avro(
                "{\"type\":\"record\",\"name\":\"S\",\"__AVRO_READ_OFFSET__\":\"5\"," + FIELDS + "}")));
    }

    @Test
    public void fallsBackToTheSchemaInfoProperties() {
        final SchemaInfo info = SchemaInfo.builder().name("S").type(SchemaType.AVRO)
                .schema(("{\"type\":\"record\",\"name\":\"S\"," + FIELDS + "}").getBytes(UTF_8))
                .properties(Collections.singletonMap("__AVRO_READ_OFFSET__", "5")).build();
        assertEquals(5, AvroReadOffset.of(info));
    }

    @Test
    public void isZeroWithoutTheProperty() {
        assertEquals(0, AvroReadOffset.of(avro("{\"type\":\"record\",\"name\":\"S\"," + FIELDS + "}")));
    }

    @Test
    public void isZeroForAnythingButAvro() {
        assertEquals(0, AvroReadOffset.of(SchemaInfo.builder().name("S").type(SchemaType.JSON)
                .schema(("{\"type\":\"record\",\"name\":\"S\",\"__AVRO_READ_OFFSET__\":\"5\"," + FIELDS + "}")
                        .getBytes(UTF_8)).build()));
        assertEquals(0, AvroReadOffset.of((SchemaInfo) null));
    }

    @Test
    public void isZeroForAnUnparseableDefinition() {
        assertEquals(0, AvroReadOffset.of(avro("not a schema __AVRO_READ_OFFSET__")));
    }

    @Test
    public void aMessageShorterThanTheOffsetIsRefused() {
        assertThrows(IOException.class, () -> AvroReadOffset.check(5, new byte[3]));
    }

    private static SchemaInfo avro(final String definition) {
        return SchemaInfo.builder().name("S").type(SchemaType.AVRO).schema(definition.getBytes(UTF_8)).build();
    }
}
