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
package org.apache.nifi.processors.pulsar.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;


public final class PropertyMappingUtils {

    private PropertyMappingUtils() {
    }

    /**
     * Given a comma-delimited list of key mappings and a value extraction function,
     * yields a map of "destkey / value" pairs.
     * 
     * @param mappings a comma-delimited list of "destkey=srckey" mappings. If no srckey is explicitly
     *   specified, then it is assumed that the destkey should also be used as the source key. 
     *     
     * @param mapper a function that, given a source key, should return the corresponding value.
     * 
     * @return the "destkey / value" map
     */
    public static Map<String, String> getMappedValues(String mappings, Function<String, String> mapper) {
        Map<String, String> values = new HashMap<String, String>();

        if (!StringUtils.isBlank(mappings)) {
            Arrays.stream(mappings.split(",", -1))
                .map((m) -> m.split("=", 2))
                .forEach((kvp) -> {
                    if (!StringUtils.isBlank(kvp[0])) {
                        String value = null;

                        if (kvp.length > 1 && !StringUtils.isBlank(kvp[1])) {
                            value = mapper.apply(kvp[1]);
                        }
                        else {
                            value = mapper.apply(kvp[0]);
                        }

                        if (value != null) {
                            values.put(kvp[0], value);
                        }
                    }
                });
        }

        return values;
    }
}
