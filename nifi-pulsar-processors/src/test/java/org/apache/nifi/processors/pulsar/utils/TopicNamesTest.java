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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;

import org.apache.pulsar.common.naming.TopicName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Checks {@link TopicNames} against the Pulsar class it stands in for. {@code TopicName} is on the test classpath
 * (pulsar-common, a runtime dependency of the client) but not one the bundle compiles against, so the two cannot
 * drift apart silently: a Pulsar upgrade that changes how a name parses fails here.
 */
public class TopicNamesTest {

    static Stream<String> names() {
        return Stream.of(
                // short names
                "my-topic", "my.topic", "my_topic:1", "t-partition-0", "t-partition-42",
                "tenant/ns/my-topic", "tenant/ns/t-partition-3",
                // fully qualified
                "persistent://public/default/my-topic",
                "non-persistent://public/default/my-topic",
                "persistent://tenant/ns/t-partition-0",
                "persistent://tenant/ns/t-partition-7",
                "non-persistent://tenant/ns/t-partition-12",
                // suffixes that are not a partition index
                "persistent://tenant/ns/t-partition-", "persistent://tenant/ns/t-partition-x",
                "persistent://tenant/ns/t-partition--1", "persistent://tenant/ns/t-partition-1a",
                "persistent://tenant/ns/a-partition-1-partition-2");
    }

    static Stream<String> invalid() {
        return Stream.of(
                "tenant/my-topic", "a/b/c/d", "persistent://tenant/my-topic", "persistent://tenant/ns/",
                "persistent://ten ant/ns/t", "bogus://tenant/ns/t",
                // V1 names, with a cluster: accepted up to Pulsar 4, rejected from Pulsar 5
                "persistent://tenant/cluster/ns/my-topic", "persistent://tenant/cluster/ns/t-partition-2",
                // which also rules out a slash in the local name
                "persistent://tenant/ns/a/b");
    }

    @ParameterizedTest
    @MethodSource("names")
    public void qualifiesLikePulsar(final String name) {
        assertEquals(TopicName.get(name).toString(), TopicNames.qualify(name), name);
    }

    @ParameterizedTest
    @MethodSource("names")
    public void findsThePartitionedTopicLikePulsar(final String name) {
        final TopicName pulsar = TopicName.get(name);
        assertEquals(pulsar.isPartitioned(), TopicNames.isPartition(name), name);
        assertEquals(pulsar.getPartitionedTopicName(), TopicNames.partitionedTopicOf(name), name);
    }

    @ParameterizedTest
    @MethodSource("invalid")
    public void rejectsWhatPulsarRejects(final String name) {
        assertThrows(IllegalArgumentException.class, () -> TopicName.get(name), "Pulsar accepts " + name);
        assertThrows(IllegalArgumentException.class, () -> TopicNames.qualify(name), name);
    }
}
