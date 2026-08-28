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
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.reporting.InitializationException;
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
 * Publishing to a topic that carries a schema (issue #34).
 * <p>
 * The publish processors created their producers with the default {@code Schema.BYTES}, which the broker
 * does not validate against the topic's schema. Publishing arbitrary content to a topic with, say, an AVRO
 * schema therefore succeeded, left the registered schema untouched, and put a message on the topic that
 * looked valid but could not be decoded. The damage landed on the consumer, which failed on that message
 * and could not get past it:
 * <pre>
 *     read 1: id=sensor-1 reading=42
 *     read 2 FAILED: AvroRuntimeException: Malformed data. Length is negative: -62
 * </pre>
 * Producers now use {@code Schema.AUTO_PRODUCE_BYTES()}, so the broker checks the payload against the
 * topic's current schema and rejects content that does not match, at publish time, where it can be routed
 * to failure and seen.
 */
public class PublishPulsarSchemaIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    /** Content that does not match the topic's schema must be rejected rather than silently accepted. */
    @Test
    public void contentThatDoesNotMatchTheTopicSchemaIsRejected() throws Exception {
        final String topic = seededTopic("mismatch");

        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.enqueue("{\"id\":\"sensor-2\",\"reading\":43}".getBytes(UTF_8));
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsar.REL_SUCCESS, 0);
        runner.assertTransferCount(PublishPulsar.REL_FAILURE, 1);
    }

    /** A schema-aware consumer must still be able to read the topic afterwards. */
    @Test
    public void aRejectedPublishLeavesTheTopicReadable() throws Exception {
        final String topic = seededTopic("readable");
        final GenericSchema<GenericRecord> schema = sensorSchema();

        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.enqueue("not an avro record at all".getBytes(UTF_8));
        runner.run(1, true);
        runner.assertTransferCount(PublishPulsar.REL_FAILURE, 1);

        try (Consumer<GenericRecord> consumer = getClient().newConsumer(schema)
                .topic(topic).subscriptionName("readable-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {

            final Message<GenericRecord> first = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull("the seeded record should still be readable", first);
            assertEquals("sensor-1", first.getValue().getField("id").toString());
            consumer.acknowledge(first);

            // nothing unreadable should have been written behind it
            assertEquals("a rejected publish must not leave an undecodable message on the topic",
                    null, consumer.receive(5, TimeUnit.SECONDS));
        }
    }

    /** Properly encoded content is accepted and reads back correctly through a schema-aware consumer. */
    @Test
    public void contentMatchingTheTopicSchemaIsPublishedAndReadable() throws Exception {
        final String topic = seededTopic("matching");
        final GenericSchema<GenericRecord> schema = sensorSchema();

        // encode a record with the topic's own schema, which is what a schema-aware writer would produce
        final byte[] encoded = schema.encode(
                schema.newRecordBuilder().set("id", "sensor-2").set("reading", 43).build());

        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.enqueue(encoded);
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsar.REL_SUCCESS, 1);
        runner.assertTransferCount(PublishPulsar.REL_FAILURE, 0);

        try (Consumer<GenericRecord> consumer = getClient().newConsumer(schema)
                .topic(topic).subscriptionName("matching-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {

            consumer.acknowledge(consumer.receive(30, TimeUnit.SECONDS));   // the seeded record

            final Message<GenericRecord> published = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull("the published record should have arrived", published);
            assertEquals("sensor-2", published.getValue().getField("id").toString());
            assertEquals(43, published.getValue().getField("reading"));
        }
    }

    /**
     * The common case is unaffected: a topic with no schema still accepts arbitrary bytes. Almost every
     * existing flow publishes to such a topic, so this is the guard against over-correcting.
     */
    @Test
    public void aTopicWithoutASchemaStillAcceptsArbitraryBytes() throws Exception {
        final String topic = "persistent://public/default/noschema-" + System.nanoTime();

        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.enqueue("plain bytes, no schema anywhere".getBytes(UTF_8));
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsar.REL_SUCCESS, 1);
        runner.assertTransferCount(PublishPulsar.REL_FAILURE, 0);

        try (Consumer<byte[]> consumer = getClient().newConsumer(Schema.BYTES)
                .topic(topic).subscriptionName("bytes-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {

            final Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals("plain bytes, no schema anywhere", new String(message.getValue(), UTF_8));
        }
    }

    // ------------------------------------------------------------------ helpers

    /** An AVRO schema, the way a schema-aware producer would leave the topic. */
    private static GenericSchema<GenericRecord> sensorSchema() {
        final RecordSchemaBuilder builder = SchemaBuilder.record("Sensor");
        builder.field("id").type(SchemaType.STRING);
        builder.field("reading").type(SchemaType.INT32);
        return Schema.generic(builder.build(SchemaType.AVRO));
    }

    /** A topic carrying an AVRO schema and one valid record. */
    private static String seededTopic(final String name) throws Exception {
        final String topic = "persistent://public/default/schema-" + name + "-" + System.nanoTime();
        final GenericSchema<GenericRecord> schema = sensorSchema();

        try (Producer<GenericRecord> seeder = getClient().newProducer(schema).topic(topic).create()) {
            seeder.send(schema.newRecordBuilder().set("id", "sensor-1").set("reading", 42).build());
        }

        return topic;
    }
}
