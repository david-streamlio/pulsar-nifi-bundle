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
import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.FIRST_MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.LAST_MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes.PROPERTY_ATTRIBUTE_PREFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression tests for "Consumer Message Batch Size" on {@link ConsumePulsarRecord}, which shares the batching logic
 * (and shared the bug) of {@link ConsumePulsar}: records from a batch of compatible messages must be written to ONE
 * FlowFile even though every message carries a unique message id. Runs with Async Enabled = false and = true.
 */
public class ConsumePulsarRecordBatchingTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/records";

    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    private boolean async;

    public void initConsumePulsarRecordBatchingTest(final boolean async) throws InitializationException {
        this.async = async;
        init();
    }

    /** Called by each test once its parameters are set, as JUnit 4 ran @Before after the constructor. */
    private void init() throws InitializationException {
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
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        // in async mode the processor keeps polling its completion service for this long after the last batch
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, async ? "1 sec" : "0 sec");
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "async={0}")
    public void tenMessagesWithUniqueMessageIdsProduceOneFlowFile(final boolean async) throws InitializationException {
        initConsumePulsarRecordBatchingTest(async);
        mockClientService.setMockMessageQueue(messages(1, 10, null));

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertBatch(flowFile, 1, 10);
        flowFile.assertAttributeEquals("topicName", TOPIC);
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "async={0}")
    public void partialBatchProducesOneFlowFileWithTheAvailableRecords(final boolean async) throws InitializationException {
        initConsumePulsarRecordBatchingTest(async);
        mockClientService.setMockMessageQueue(messages(1, 4, null));

        runner.run(1, true);

        assertBatch(successFlowFiles(1).get(0), 1, 4);
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "async={0}")
    public void moreMessagesThanBatchSizeProduceOneFlowFilePerBatch(final boolean async) throws InitializationException {
        initConsumePulsarRecordBatchingTest(async);
        mockClientService.setMockMessageQueue(messages(1, 25, null));

        runner.run(3, true);

        List<MockFlowFile> flowFiles = successFlowFiles(3);
        assertBatch(flowFiles.get(0), 1, 10);
        assertBatch(flowFiles.get(1), 11, 20);
        assertBatch(flowFiles.get(2), 21, 25);
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "async={0}")
    public void uniquePerMessagePropertiesDoNotSplitTheBatch(final boolean async) throws InitializationException {
        initConsumePulsarRecordBatchingTest(async);
        List<Message<GenericRecord>> messages = new ArrayList<>();
        for (int n = 1; n <= 10; n++) {
            Map<String, String> properties = new HashMap<>();
            properties.put("source", "crm");
            properties.put("trace-id", "trace-" + n);
            messages.add(message(n, properties));
        }
        mockClientService.setMockMessageQueue(messages);

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertBatch(flowFile, 1, 10);
        flowFile.assertAttributeEquals(PROPERTY_ATTRIBUTE_PREFIX + "source", "crm");
        flowFile.assertAttributeNotExists(PROPERTY_ATTRIBUTE_PREFIX + "trace-id");
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "async={0}")
    public void singleMessageFlowFileKeepsThePreviousAttributeContract(final boolean async) throws InitializationException {
        initConsumePulsarRecordBatchingTest(async);
        mockClientService.setMockMessageQueue(messages(5, 5, Collections.singletonMap("source", "crm")));

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertBatch(flowFile, 5, 5);
        flowFile.assertAttributeEquals(MESSAGE_ID_ATTRIBUTE, messageId(5));
        flowFile.assertAttributeEquals(PROPERTY_ATTRIBUTE_PREFIX + "source", "crm");
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "async={0}")
    public void identicalMappedAttributeKeepsMessagesInOneFlowFile(final boolean async) throws InitializationException {
        initConsumePulsarRecordBatchingTest(async);
        runner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "tenant");
        mockClientService.setMockMessageQueue(messages(1, 3, Collections.singletonMap("tenant", "A")));

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertBatch(flowFile, 1, 3);
        flowFile.assertAttributeEquals("tenant", "A");
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "async={0}")
    public void changedMappedAttributeStartsANewFlowFile(final boolean async) throws InitializationException {
        initConsumePulsarRecordBatchingTest(async);
        runner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "tenant");
        List<Message<GenericRecord>> messages = new ArrayList<>();
        messages.addAll(messages(1, 2, Collections.singletonMap("tenant", "A")));
        messages.addAll(messages(3, 4, Collections.singletonMap("tenant", "B")));
        mockClientService.setMockMessageQueue(messages);

        runner.run(1, true);

        List<MockFlowFile> flowFiles = successFlowFiles(2);
        assertBatch(flowFiles.get(0), 1, 2);
        flowFiles.get(0).assertAttributeEquals("tenant", "A");
        assertBatch(flowFiles.get(1), 3, 4);
        flowFiles.get(1).assertAttributeEquals("tenant", "B");
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private static String messageId(final int n) {
        return "5678:" + n + ":-1";
    }

    /** One CSV record per message, parsed by MockRecordParser as (name, age). */
    private static Message<GenericRecord> message(final int n, final Map<String, String> properties) {
        return new MockPulsarMessage<>(TOPIC, ("Name" + n + "," + n).getBytes(UTF_8), messageId(n), properties, null);
    }

    private static List<Message<GenericRecord>> messages(final int first, final int last, final Map<String, String> properties) {
        return IntStream.rangeClosed(first, last).mapToObj(n -> message(n, properties)).collect(Collectors.toList());
    }

    private List<MockFlowFile> successFlowFiles(final int expectedCount) {
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS, expectedCount);
        return runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
    }

    /** Asserts that the FlowFile holds exactly the records of messages first..last, in order, with coherent batch attributes. */
    private static void assertBatch(final MockFlowFile flowFile, final int first, final int last) {
        final int count = last - first + 1;

        flowFile.assertAttributeEquals(ConsumePulsarRecord.MSG_COUNT, String.valueOf(count));
        flowFile.assertContentEquals(IntStream.rangeClosed(first, last)
                .mapToObj(n -> "\"Name" + n + "\",\"" + n + "\"\n")
                .collect(Collectors.joining()));

        flowFile.assertAttributeEquals(FIRST_MESSAGE_ID_ATTRIBUTE, messageId(first));
        flowFile.assertAttributeEquals(LAST_MESSAGE_ID_ATTRIBUTE, messageId(last));
        if (count == 1) {
            flowFile.assertAttributeEquals(MESSAGE_ID_ATTRIBUTE, messageId(first));
        } else {
            flowFile.assertAttributeNotExists(MESSAGE_ID_ATTRIBUTE);
        }
    }
}
