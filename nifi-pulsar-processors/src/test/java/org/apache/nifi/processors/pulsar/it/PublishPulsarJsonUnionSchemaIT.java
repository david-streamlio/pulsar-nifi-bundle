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
import static org.junit.Assert.assertNull;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
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
 * Nullable fields on a JSON-schema topic.
 * <p>
 * The JSON encoding added for #34 phase three writes a union's resolved branch bare rather than in Avro's
 * {@code {"string":"x"}} wrapper, on the reasoning that Pulsar's {@code GenericJsonRecord} reads ordinary
 * Jackson JSON. That was inferred from reading the client rather than measured, and an optional field -
 * {@code ["null","string"]} - is the most common shape in a real schema, so it is worth measuring: if the
 * inference is wrong, this is silent corruption of exactly the kind #34 is about.
 */
public class PublishPulsarJsonUnionSchemaIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        // A real JSON reader, because the mock parser turns an empty CSV field into "" and can never
        // produce a genuine null - which is the case that actually exercises the union encoding.
        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);

        final MockRecordWriter writer = new MockRecordWriter("id, note, reading");
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(PublishPulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");
    }

    /** An optional field carrying a value, and the same field carrying null, must both round-trip. */
    @Test
    public void optionalFieldsRoundTripThroughAJsonSchemaTopic() throws Exception {
        final String topic = seededTopic();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);

        // record 1 has the optional field set; record 2 omits it entirely, so it is genuinely null
        runner.enqueue(("[{\"id\":\"sensor-2\",\"note\":\"hello\",\"reading\":43},"
                + "{\"id\":\"sensor-3\",\"reading\":44}]").getBytes(UTF_8));
        runner.run(1, true);

        runner.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);

        try (Consumer<GenericRecord> consumer = getClient().newConsumer(sensorSchema())
                .topic(topic).subscriptionName("union-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {

            consumer.acknowledge(consumer.receive(30, TimeUnit.SECONDS));   // the seeded record

            final Message<GenericRecord> withValue = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull("the record with an optional value should have arrived", withValue);
            assertEquals("sensor-2", withValue.getValue().getField("id").toString());
            assertEquals("an optional field carrying a value decoded wrongly - the union encoding is off",
                    "hello", String.valueOf(withValue.getValue().getField("note")));
            assertEquals(43, withValue.getValue().getField("reading"));
            consumer.acknowledge(withValue);

            final Message<GenericRecord> withNull = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull("the record with a null optional should have arrived", withNull);
            assertEquals("sensor-3", withNull.getValue().getField("id").toString());
            assertNull("an absent optional field should decode as null", withNull.getValue().getField("note"));
            assertEquals(44, withNull.getValue().getField("reading"));
        }
    }

    // ------------------------------------------------------------------ helpers

    /** id required; note optional, i.e. a ["null","string"] union; reading required. */
    private static GenericSchema<GenericRecord> sensorSchema() {
        final RecordSchemaBuilder builder = SchemaBuilder.record("Sensor");
        builder.field("id").type(SchemaType.STRING);
        builder.field("note").type(SchemaType.STRING).optional();
        builder.field("reading").type(SchemaType.INT32);
        return Schema.generic(builder.build(SchemaType.JSON));
    }

    private static String seededTopic() throws Exception {
        final String topic = "persistent://public/default/json-union-" + System.nanoTime();
        final GenericSchema<GenericRecord> schema = sensorSchema();

        try (Producer<GenericRecord> seeder = getClient().newProducer(schema).topic(topic).create()) {
            seeder.send(schema.newRecordBuilder()
                    .set("id", "sensor-1").set("note", "seeded").set("reading", 42).build());
        }

        return topic;
    }
}
