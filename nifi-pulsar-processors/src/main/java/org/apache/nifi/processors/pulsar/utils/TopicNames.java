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

/**
 * The two operations on Pulsar topic names this bundle needs, without {@code org.apache.pulsar.common.naming.TopicName}.
 *
 * <p>That class is not part of the client API. Up to Pulsar 4 the client jars shipped it under its own name; from
 * Pulsar 5 they relocate it under {@code org.apache.pulsar.shade}, so it can no longer be compiled against. The rules
 * here are the subset of its parsing that these two operations depend on, and {@code TopicNamesTest} checks them
 * against the real class.</p>
 */
public final class TopicNames {

    static final String PARTITION_SUFFIX = "-partition-";

    private static final String DEFAULT_DOMAIN = "persistent";
    private static final String DEFAULT_TENANT_NAMESPACE = "public/default/";

    private TopicNames() {
    }

    /**
     * The fully-qualified name: {@code topic} becomes {@code persistent://public/default/topic}, and
     * {@code tenant/namespace/topic} becomes {@code persistent://tenant/namespace/topic}. A name that already has a
     * domain is returned unchanged.
     *
     * @throws IllegalArgumentException if the name is not one Pulsar can parse
     */
    public static String qualify(final String topic) {
        final String complete;
        if (topic.contains("://")) {
            complete = topic;
        } else {
            final String[] parts = topic.split("/", -1);
            if (parts.length == 1) {
                complete = DEFAULT_DOMAIN + "://" + DEFAULT_TENANT_NAMESPACE + topic;
            } else if (parts.length == 3) {
                complete = DEFAULT_DOMAIN + "://" + topic;
            } else {
                throw new IllegalArgumentException("Invalid short topic name '" + topic
                        + "': expected <tenant>/<namespace>/<topic> or <topic>");
            }
        }
        validate(complete);
        return complete;
    }

    /**
     * The partitioned topic a partition belongs to, fully qualified: {@code persistent://t/n/topic-partition-3}
     * becomes {@code persistent://t/n/topic}. A name that is not a partition is returned qualified.
     *
     * @throws IllegalArgumentException if the name is not one Pulsar can parse
     */
    public static String partitionedTopicOf(final String topic) {
        final String complete = qualify(topic);
        return partitionIndex(complete) >= 0 ? complete.substring(0, complete.lastIndexOf(PARTITION_SUFFIX)) : complete;
    }

    /** Whether the name is a partition of a partitioned topic, i.e. ends in {@code -partition-<n>}, n &ge; 0. */
    public static boolean isPartition(final String topic) {
        return partitionIndex(topic) >= 0;
    }

    private static int partitionIndex(final String topic) {
        final int at = topic.lastIndexOf(PARTITION_SUFFIX);
        if (at < 0) {
            return -1;
        }
        final String index = topic.substring(at + PARTITION_SUFFIX.length());
        if (index.isEmpty() || !index.chars().allMatch(Character::isDigit)) {
            return -1;
        }
        try {
            return Integer.parseInt(index);
        } catch (final NumberFormatException e) {
            return -1;
        }
    }

    private static void validate(final String complete) {
        final int sep = complete.indexOf("://");
        final String domain = complete.substring(0, sep);
        if (!domain.equals("persistent") && !domain.equals("non-persistent")) {
            throw new IllegalArgumentException("Invalid topic domain '" + domain + "' in '" + complete + "'");
        }
        // tenant/namespace/topic. Pulsar 5 no longer accepts the V1 tenant/cluster/namespace/topic form, and so no
        // longer accepts a '/' in the local name, which would read as one.
        final String[] parts = complete.substring(sep + 3).split("/", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid topic name '" + complete
                    + "': expected <domain>://<tenant>/<namespace>/<topic>");
        }
        for (int i = 0; i < parts.length - 1; i++) {
            if (!isValidName(parts[i])) {
                throw new IllegalArgumentException("Invalid topic name '" + complete + "'");
            }
        }
        if (parts[parts.length - 1].isBlank()) {
            throw new IllegalArgumentException("Invalid topic name '" + complete + "': local name must not be blank");
        }
    }

    /** Pulsar's NamedEntity rule for tenant and namespace names: {@code [-=:.\w]+}. */
    private static boolean isValidName(final String name) {
        return !name.isEmpty() && name.matches("[-=:.\\w]+");
    }
}
