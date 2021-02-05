/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar;

import java.util.HashMap;
import java.util.Map;

import java.util.function.Function;

import static org.apache.nifi.processors.pulsar.PropertyMappingUtils.getMappedValues;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPropertyMappingUtils {
    private Map<String, String> values = new HashMap<>();    
    private Function<String, String> func = new Function<String, String>() {
        public String apply(String s) {
            return values.get(s);
        }
    };

    @Before
    public void setUp() {
        values.clear();
    }

    @Test
    public void blankMappingsTest() {
        assertEquals(0, getMappedValues(" ", func).size());
    }

    @Test
    public void nullMappingsTest() {
        assertEquals(0, getMappedValues(null, func).size());
    }

    @Test
    public void blankMappedAttributeNameTest() {
        values.put("prop", "val");

        Map<String, String> props = getMappedValues("prop= ", func);
        assertEquals(1, props.size());
        assertEquals("val", props.get("prop"));
    }

    @Test
    public void mappedAttributeTest() {
        values.put("attr", "val");

        Map<String, String> props = getMappedValues("prop=attr", func);
        assertEquals(1, props.size());
        assertEquals("val", props.get("prop"));
    }

    @Test
    public void blankPropertyNameTest() {
        Map<String, String> props = getMappedValues(" =attr", func);
        assertEquals(0, props.size());
    }

    @Test
    public void propertyNameOnlyTest() {
        values.put("prop", "val");

        Map<String, String> props = getMappedValues("prop", func);
        assertEquals(1, props.size());
        assertEquals("val", props.get("prop"));
    }
}
