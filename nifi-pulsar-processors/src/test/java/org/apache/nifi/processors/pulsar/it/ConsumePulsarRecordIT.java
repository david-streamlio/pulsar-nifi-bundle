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
package org.apache.nifi.processors.pulsar.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.junit.Test;
import org.testcontainers.containers.Container;

/**
 * ConsumePulsarRecord against a real broker. What a message carries as its reader schema - and whether
 * that schema has any schema info at all - is decided by the broker, so this cannot be shown with the mock
 * client, whose messages report no reader schema.
 */
public class ConsumePulsarRecordIT extends AbstractPulsarIT {

    private static final int MESSAGES = 30;
    private static int topicCounter = 0;

    /**
     * The case from the issue. A topic never given a schema yields a reader schema whose SchemaInfo is
     * null; before the fix every trigger threw NullPointerException out of onTrigger and nothing was ever
     * consumed from such a topic.
     */
    @Test
    public void aTopicWithoutASchemaIsConsumed() throws Exception {
        final String topic = topic("schemaless");
        publish(topic, messages());

        final TestRunner runner = consumer(topic);
        final int records = consumeUntil(runner, MESSAGES);

        assertEquals(MESSAGES, records);
        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
            flowFile.assertAttributeNotExists("avro.schema");
        }
    }

    /** The common production shape - a topic carrying a STRING schema - keeps working as before. */
    @Test
    public void aTopicWithAStringSchemaIsConsumed() throws Exception {
        final String topic = topic("string-schema");
        try (Producer<String> producer = getClient().newProducer(Schema.STRING).topic(topic).create()) {
            for (final String message : messages()) {
                producer.send(message);
            }
        }

        final TestRunner runner = consumer(topic);
        final int records = consumeUntil(runner, MESSAGES);

        assertEquals(MESSAGES, records);
        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
    }

    /** The partitions of a partitioned topic are read as one logical topic (#141), on a real broker too. */
    @Test
    public void aPartitionedTopicIsReadAsOneLogicalTopic() throws Exception {
        final String topic = topic("partitioned");
        final Container.ExecResult result = PULSAR.execInContainer("bin/pulsar-admin", "topics", "create-partitioned-topic", topic, "-p", "3");
        assertEquals("pulsar-admin failed: " + result.getStderr(), 0, result.getExitCode());
        publish(topic, messages());

        final TestRunner runner = consumer(topic);
        final int records = consumeUntil(runner, MESSAGES);

        assertEquals(MESSAGES, records);
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS)) {
            assertEquals("a partition must be reported as its logical topic", topic, flowFile.getAttribute("topicName"));
        }
    }

    private static String topic(final String name) {
        return "persistent://public/default/consume-" + name + "-" + (topicCounter++) + "-" + System.nanoTime();
    }

    /** {@code MESSAGES} JSON payloads of the same shape, so they share one record set. */
    private static String[] messages() {
        final String[] messages = new String[MESSAGES];
        for (int seq = 1; seq <= MESSAGES; seq++) {
            messages[seq - 1] = "{\"device\":\"d" + (seq % 3) + "\",\"seq\":" + seq + "}";
        }
        return messages;
    }

    private TestRunner consumer(final String topic) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "100");
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
        return runner;
    }

    /**
     * Triggers the processor until {@code expected} records have reached success, then stops it. The
     * processor polls with {@code receive(0, SECONDS)}, so the first triggers after the consumer connects
     * can legitimately return nothing while the broker is still delivering - a fixed trigger count would
     * "lose" a tail of messages that simply had not arrived yet.
     */
    private static int consumeUntil(final TestRunner runner, final int expected) throws Exception {
        final int[] records = {0};
        runner.run(1, false, true);
        await(expected + " records to be consumed", () -> {
            runner.run(1, false, false);
            records[0] = 0;
            final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
            for (final MockFlowFile flowFile : flowFiles) {
                records[0] += Integer.parseInt(flowFile.getAttribute("record.count"));
            }
            return records[0] >= expected;
        });
        runner.run(1, true, false);
        return records[0];
    }
}
