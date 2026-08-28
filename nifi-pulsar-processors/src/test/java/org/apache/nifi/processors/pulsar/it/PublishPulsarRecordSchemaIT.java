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
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;

/**
 * Publishing NiFi records to a topic that carries a schema (issue #34, phase two).
 * <p>
 * Phase one stopped content that does not match a topic's schema from being written at all. That made the
 * silent corruption impossible, but it also left PublishPulsarRecord unable to write to such a topic: its
 * Record Writer emits JSON, CSV or Avro-with-header, none of which the broker accepts for an AVRO topic.
 * <p>
 * With <em>Message Schema Strategy = Topic Schema</em> each record is converted to the topic's own Avro
 * schema and encoded the way Pulsar's AVRO schema encodes, so it is accepted and a schema-aware consumer
 * can decode it.
 */
public class PublishPulsarRecordSchemaIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.STRING);
        reader.addSchemaField("reading", RecordFieldType.INT);
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);

        final MockRecordWriter writer = new MockRecordWriter("id, reading");
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    /** The point of phase two: records reach a schema-bearing topic and decode correctly. */
    @Test
    public void recordsAreEncodedWithTheTopicSchema() throws Exception {
        final String topic = seededTopic("record-encode");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(PublishPulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");

        runner.enqueue("sensor-2,43\nsensor-3,44".getBytes(UTF_8));
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);

        // Collected as a set, not a sequence: PublisherLease dispatches each record through
        // CompletableFuture.supplyAsync(), so records from one FlowFile race each other and can land out of
        // order. That is a separate defect in the publisher, not something this strategy introduces.
        final java.util.Set<String> received = new java.util.HashSet<>();

        try (Consumer<GenericRecord> consumer = getClient().newConsumer(sensorSchema())
                .topic(topic).subscriptionName("record-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {

            for (int n = 1; n <= 3; n++) {
                final Message<GenericRecord> msg = consumer.receive(30, TimeUnit.SECONDS);
                assertNotNull("expected 3 decodable messages, got " + received.size(), msg);
                received.add(msg.getValue().getField("id") + "=" + msg.getValue().getField("reading"));
                consumer.acknowledge(msg);
            }
        }

        assertEquals("every record should have been encoded with the topic's schema and decoded by a "
                        + "schema-aware consumer",
                new java.util.HashSet<>(java.util.Arrays.asList("sensor-1=42", "sensor-2=43", "sensor-3=44")),
                received);
    }

    /**
     * The default strategy is unchanged, and against a schema-bearing topic it now fails visibly rather
     * than writing something no consumer can read. This is what makes the new strategy necessary.
     */
    @Test
    public void theRecordWriterStrategyIsRejectedByASchemaBearingTopic() throws Exception {
        final String topic = seededTopic("writer-rejected");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        // Message Schema Strategy left at its default, Record Writer

        runner.enqueue("sensor-2,43".getBytes(UTF_8));
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 1);
    }

    /**
     * Asking for the topic's schema on a topic that has none falls back to the Record Writer, so turning
     * the strategy on does not break flows whose topics are unschematised.
     */
    @Test
    public void topicSchemaStrategyFallsBackWhenTheTopicHasNoSchema() throws Exception {
        final String topic = "persistent://public/default/record-noschema-" + System.nanoTime();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(PublishPulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");

        runner.enqueue("sensor-9,99".getBytes(UTF_8));
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);

        try (Consumer<byte[]> consumer = getClient().newConsumer(Schema.BYTES)
                .topic(topic).subscriptionName("fallback-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {

            final Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals("\"sensor-9\",\"99\"\n", new String(message.getValue(), UTF_8));
        }
    }

    // ------------------------------------------------------------------ helpers

    private static GenericSchema<GenericRecord> sensorSchema() {
        final RecordSchemaBuilder builder = SchemaBuilder.record("Sensor");
        builder.field("id").type(SchemaType.STRING);
        builder.field("reading").type(SchemaType.INT32);
        return Schema.generic(builder.build(SchemaType.AVRO));
    }

    private static String seededTopic(final String name) throws Exception {
        final String topic = "persistent://public/default/rec-schema-" + name + "-" + System.nanoTime();
        final GenericSchema<GenericRecord> schema = sensorSchema();

        try (Producer<GenericRecord> seeder = getClient().newProducer(schema).topic(topic).create()) {
            seeder.send(schema.newRecordBuilder().set("id", "sensor-1").set("reading", 42).build());
        }

        return topic;
    }
}
