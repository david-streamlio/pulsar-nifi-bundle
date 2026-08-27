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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Regression tests for "Consumer Message Batch Size" on {@link ConsumePulsar}: a batch of compatible messages must
 * end up in ONE FlowFile even though every Pulsar message carries a unique message id (and possibly unique
 * properties). Before the fix the message id took part in the "may these messages share a FlowFile" comparison, so
 * every message produced its own FlowFile regardless of the configured batch size.
 * <p>
 * Every scenario runs with {@code Async Enabled = false} and {@code = true}: both code paths implement the batching.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarBatchingTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/events";

    @Parameters(name = "async={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    private final boolean async;

    public ConsumePulsarBatchingTest(final boolean async) {
        this.async = async;
    }

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        // Mapped FlowFile Attributes intentionally left at its (empty) default
    }

    // ------------------------------------------------------------------------------------------------ Test 1 & 4

    @Test
    public void tenMessagesWithUniqueMessageIdsProduceOneFlowFile() throws PulsarClientException {
        mockClientService.setMockMessageQueue(messages(1, 10, null));

        runner.run(1, true);

        assertBatch(successFlowFiles(1).get(0), 1, 10);
        // the loop stops on the batch-size counter, so receive() is not called an 11th time
        verify(mockClientService.getMockConsumer(), times(10)).receive(0, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------------------------------------------ Test 2

    @Test
    public void partialBatchProducesOneFlowFileWithTheAvailableMessages() {
        mockClientService.setMockMessageQueue(messages(1, 4, null));

        runner.run(1, true);

        assertBatch(successFlowFiles(1).get(0), 1, 4);
    }

    // ------------------------------------------------------------------------------------------------ Test 3

    @Test
    public void moreMessagesThanBatchSizeProduceOneFlowFilePerBatch() {
        mockClientService.setMockMessageQueue(messages(1, 25, null));

        runner.run(3, true);

        List<MockFlowFile> flowFiles = successFlowFiles(3);
        assertBatch(flowFiles.get(0), 1, 10);
        assertBatch(flowFiles.get(1), 11, 20);
        assertBatch(flowFiles.get(2), 21, 25);
    }

    // ------------------------------------------------------------------------------------------------ Test 4

    @Test
    public void uniquePerMessagePropertiesDoNotSplitTheBatch() {
        List<Message<GenericRecord>> messages = new ArrayList<>();
        for (int n = 1; n <= 10; n++) {
            Map<String, String> properties = new HashMap<>();
            properties.put("source", "device-gateway");
            properties.put("trace-id", "trace-" + n);
            messages.add(message(n, properties, null));
        }
        mockClientService.setMockMessageQueue(messages);

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertBatch(flowFile, 1, 10);
        // a property shared by every message of the batch is kept, a per-message one is not
        flowFile.assertAttributeEquals(PROPERTY_ATTRIBUTE_PREFIX + "source", "device-gateway");
        flowFile.assertAttributeNotExists(PROPERTY_ATTRIBUTE_PREFIX + "trace-id");
    }

    @Test
    public void singleMessageFlowFileKeepsThePreviousAttributeContract() {
        mockClientService.setMockMessageQueue(messages(7, 7, Collections.singletonMap("source", "device-gateway")));

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertBatch(flowFile, 7, 7);
        flowFile.assertAttributeEquals(MESSAGE_ID_ATTRIBUTE, messageId(7));
        flowFile.assertAttributeEquals(PROPERTY_ATTRIBUTE_PREFIX + "source", "device-gateway");
    }

    // ------------------------------------------------------------------------------------------------ Test 5

    @Test
    public void identicalMappedAttributeKeepsMessagesInOneFlowFile() {
        runner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "tenant");
        mockClientService.setMockMessageQueue(messages(1, 3, Collections.singletonMap("tenant", "A")));

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertBatch(flowFile, 1, 3);
        flowFile.assertAttributeEquals("tenant", "A");
        flowFile.assertAttributeEquals(PROPERTY_ATTRIBUTE_PREFIX + "tenant", "A");
    }

    // ------------------------------------------------------------------------------------------------ Test 6

    @Test
    public void changedMappedAttributeStartsANewFlowFile() {
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

    // ------------------------------------------------------------------------------------------------ Test 7

    @Test
    public void newlineDemarcatorSeparatesMessagesWithoutTrailingDelimiter() {
        mockClientService.setMockMessageQueue(Arrays.asList(
                message(1, "message1"), message(2, "message2"), message(3, "message3")));

        runner.run(1, true);

        MockFlowFile flowFile = successFlowFiles(1).get(0);
        flowFile.assertContentEquals("message1\nmessage2\nmessage3");
        flowFile.assertAttributeEquals(ConsumePulsar.MSG_COUNT, "3");
    }

    @Test
    public void customDemarcatorIsHonoured() {
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "|#|");
        mockClientService.setMockMessageQueue(Arrays.asList(
                message(1, "message1"), message(2, "message2"), message(3, "message3")));

        runner.run(1, true);

        successFlowFiles(1).get(0).assertContentEquals("message1|#|message2|#|message3");
    }

    // ------------------------------------------------------------------------------------------------ Shared subscription

    @Test
    public void sharedSubscriptionBatchesMessagesToo() throws PulsarClientException {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        mockClientService.setMockMessageQueue(messages(1, 10, null));

        runner.run(1, true);

        assertBatch(successFlowFiles(1).get(0), 1, 10);
        // shared subscriptions acknowledge every message individually
        if (async) {
            verify(mockClientService.getMockConsumer(), times(10)).acknowledgeAsync(any(Message.class));
        } else {
            verify(mockClientService.getMockConsumer(), times(10)).acknowledge(any(Message.class));
        }
    }

    // ------------------------------------------------------------------------------------------------ helpers

    /** Message id of the n-th message: "ledgerId:entryId:partitionIndex", as delivered by a partitioned topic. */
    private static String messageId(final int n) {
        return "1234:" + n + ":" + (n % 3);
    }

    private static Message<GenericRecord> message(final int n, final Map<String, String> properties, final String key) {
        return new MockPulsarMessage<>(TOPIC + "-partition-" + (n % 3), ("{\"id\":" + n + "}").getBytes(UTF_8),
                messageId(n), properties, key);
    }

    private static Message<GenericRecord> message(final int n, final String content) {
        return new MockPulsarMessage<>(TOPIC + "-partition-" + (n % 3), content.getBytes(UTF_8), messageId(n), null, null);
    }

    private static List<Message<GenericRecord>> messages(final int first, final int last, final Map<String, String> properties) {
        return IntStream.rangeClosed(first, last).mapToObj(n -> message(n, properties, null)).collect(Collectors.toList());
    }

    private List<MockFlowFile> successFlowFiles(final int expectedCount) {
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS, expectedCount);
        return runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
    }

    /** Asserts that the FlowFile holds exactly the messages first..last, in order, with coherent batch attributes. */
    private static void assertBatch(final MockFlowFile flowFile, final int first, final int last) {
        final int count = last - first + 1;

        flowFile.assertAttributeEquals(ConsumePulsar.MSG_COUNT, String.valueOf(count));
        flowFile.assertContentEquals(IntStream.rangeClosed(first, last)
                .mapToObj(n -> "{\"id\":" + n + "}")
                .collect(Collectors.joining("\n")));

        flowFile.assertAttributeEquals(FIRST_MESSAGE_ID_ATTRIBUTE, messageId(first));
        flowFile.assertAttributeEquals(LAST_MESSAGE_ID_ATTRIBUTE, messageId(last));
        if (count == 1) {
            flowFile.assertAttributeEquals(MESSAGE_ID_ATTRIBUTE, messageId(first));
        } else {
            flowFile.assertAttributeNotExists(MESSAGE_ID_ATTRIBUTE);
        }
    }
}
