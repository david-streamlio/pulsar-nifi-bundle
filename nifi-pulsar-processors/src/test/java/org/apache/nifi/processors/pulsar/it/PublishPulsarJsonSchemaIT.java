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
 * Publishing NiFi records to a topic carrying a JSON schema (issue #34, phase three).
 * <p>
 * Phase two encodes records with the topic's schema when that schema is AVRO. A JSON-schema topic
 * falls through to the Record Writer, whose output the broker then rejects, so such a topic cannot
 * currently be written to at all with Message Schema Strategy = Topic Schema.
 */
public class PublishPulsarJsonSchemaIT extends AbstractPulsarIT {

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

    /** Records must reach a JSON-schema topic and decode through a schema-aware consumer. */
    @Test
    public void recordsAreEncodedWithAJsonTopicSchema() throws Exception {
        final String topic = seededJsonTopic();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(PublishPulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");

        runner.enqueue("sensor-2,43".getBytes(UTF_8));
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);

        try (Consumer<GenericRecord> consumer = getClient().newConsumer(jsonSchema())
                .topic(topic).subscriptionName("json-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {

            consumer.acknowledge(consumer.receive(30, TimeUnit.SECONDS));   // the seeded record

            final Message<GenericRecord> published = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull("the published record should have arrived", published);
            assertEquals("sensor-2", published.getValue().getField("id").toString());
            assertEquals(43, published.getValue().getField("reading"));
        }
    }

    // ------------------------------------------------------------------ helpers

    private static GenericSchema<GenericRecord> jsonSchema() {
        final RecordSchemaBuilder builder = SchemaBuilder.record("Sensor");
        builder.field("id").type(SchemaType.STRING);
        builder.field("reading").type(SchemaType.INT32);
        return Schema.generic(builder.build(SchemaType.JSON));
    }

    private static String seededJsonTopic() throws Exception {
        final String topic = "persistent://public/default/json-schema-" + System.nanoTime();
        final GenericSchema<GenericRecord> schema = jsonSchema();

        try (Producer<GenericRecord> seeder = getClient().newProducer(schema).topic(topic).create()) {
            seeder.send(schema.newRecordBuilder().set("id", "sensor-1").set("reading", 42).build());
        }

        return topic;
    }
}
