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
package org.apache.nifi.processors.pulsar.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Optional;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * A message from a topic that has no schema must be consumed like any other.
 * <p>
 * The consumer subscribes with {@code Schema.AUTO_CONSUME()}. For such a message the reader schema is
 * present - it is the {@code AutoConsumeSchema} itself - but its {@code SchemaInfo} is {@code null}, and
 * the attribute code dereferenced it: every trigger threw {@code NullPointerException} out of
 * {@code onTrigger}, nothing was acknowledged, and the batch came back to fail again. The mock message
 * used everywhere else returns an empty reader schema, which is why no unit test could show it.
 */
public class ConsumePulsarRecordSchemalessTopicTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/schemaless";

    /** What a real message from a schema-less topic looks like: a reader schema without schema info. */
    private static final class SchemalessMessage extends MockPulsarMessage<GenericRecord> {
        private SchemalessMessage(final String payload, final int id) {
            super(TOPIC, payload.getBytes(UTF_8), "1234:" + id + ":0", null, null);
        }

        @Override
        public Optional<Schema<?>> getReaderSchema() {
            final Schema<?> autoConsumeWithoutSchema = mock(Schema.class);   // getSchemaInfo() returns null
            return Optional.of(autoConsumeWithoutSchema);
        }
    }

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addPulsarClientService();

        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService("record-reader", readerService);
        runner.enableControllerService(readerService);

        final MockRecordWriter writerService = new MockRecordWriter("name, age");
        runner.addControllerService("record-writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
    }

    @Test
    public void messagesFromATopicWithoutASchemaAreConsumed() {
        mockClientService.setMockMessageQueue(Arrays.asList(
                new SchemalessMessage("Name1,1", 1), new SchemalessMessage("Name2,2", 2), new SchemalessMessage("Name3,3", 3)));

        // Before the fix this threw NullPointerException out of onTrigger.
        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS).get(0);
        assertEquals("3", flowFile.getAttribute("record.count"));
        flowFile.assertAttributeNotExists("avro.schema");
    }
}
