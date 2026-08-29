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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * A record set is written with the schema it was opened with, so a message whose schema differs has to
 * start a new record set - the same rule that already applies to a change in the mapped attributes.
 * <p>
 * ConsumePulsarRecord used to create the set's writer from the schema of its first message and write every
 * later message of the set through it. With a reader that infers the schema from the payload - the default
 * of JsonTreeReader - any field the first message did not have was silently dropped, any field it had that
 * a later message lacked came out as null, and a field whose type differed made the writer throw out of
 * onTrigger. These tests run the real JsonTreeReader and JsonRecordSetWriter, because the mock parser has a
 * fixed schema and cannot show any of this.
 */
public class ConsumePulsarRecordSchemaChangeTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/telemetry";

    private static final String MESSAGE_1 = "{\"id\":1,\"a\":\"x\",\"nested\":{\"p\":1},\"tags\":[{\"k\":\"t1\"}]}";
    private static final String MESSAGE_2 = "{\"id\":2,\"a\":\"y\",\"extra\":\"only-in-2\",\"nested\":{\"p\":2,\"q\":\"only-in-2\"},"
            + "\"tags\":[{\"k\":\"t2\",\"m\":true}]}";
    private static final String MESSAGE_3 = "{\"id\":3,\"b\":true}";

    private JsonTreeReader reader;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addPulsarClientService();

        reader = new JsonTreeReader();
        runner.addControllerService("record-reader", reader);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
    }

    /**
     * The case from the issue: three messages, each with fields the others do not have. Each gets its own
     * record set and keeps every field. Before the fix all three were written through the first message's
     * schema as one FlowFile, and {@code extra}, {@code nested.q}, {@code tags[].m} and {@code b} were gone.
     */
    @Test
    public void aMessageWithADifferentSchemaStartsANewRecordSet() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, MESSAGE_2, MESSAGE_3));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        final List<MockFlowFile> flowFiles = successFlowFiles(3);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("1", flowFile.getAttribute("record.count"));
        }

        final String second = new String(flowFiles.get(1).toByteArray(), UTF_8);
        assertTrue("a top-level field the first message lacks was dropped: " + second, second.contains("\"extra\":\"only-in-2\""));
        assertTrue("a nested field the first message lacks was dropped: " + second, second.contains("\"q\":\"only-in-2\""));
        assertTrue("an array element field the first message lacks was dropped: " + second, second.contains("\"m\":true"));
        final String third = new String(flowFiles.get(2).toByteArray(), UTF_8);
        assertTrue("a field only the last message has was dropped: " + third, third.contains("\"b\":true"));
        assertFalse("a field the last message does not have was invented: " + third, third.contains("\"a\""));
    }

    /**
     * A field whose type differs between messages is a schema change like any other. Before the fix the
     * second value was coerced to the first message's type, and the writer threw NumberFormatException out
     * of onTrigger - the trigger failed, nothing was acknowledged, and the batch came back to fail again.
     */
    @Test
    public void aTypeConflictStartsANewRecordSetInsteadOfFailingTheTrigger() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages("{\"id\":1,\"v\":1}", "{\"id\":2,\"v\":\"text\"}"));

        runner.run(1, true);

        final List<MockFlowFile> flowFiles = successFlowFiles(2);
        assertEquals("[{\"id\":1,\"v\":1}]", new String(flowFiles.get(0).toByteArray(), UTF_8));
        assertEquals("[{\"id\":2,\"v\":\"text\"}]", new String(flowFiles.get(1).toByteArray(), UTF_8));
    }

    /** Messages with the same shape keep sharing one record set, exactly as before. */
    @Test
    public void messagesWithTheSameSchemaShareOneRecordSet() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, MESSAGE_1.replace("\"id\":1", "\"id\":2"), MESSAGE_1.replace("\"id\":1", "\"id\":3")));

        runner.run(1, true);

        final MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertEquals("3", flowFile.getAttribute("record.count"));
        assertEquals("3", flowFile.getAttribute(ConsumePulsarRecord.MSG_COUNT));
    }

    /**
     * The trade-off, pinned so it is not a surprise: the set closes on every change, so messages that
     * alternate between two shapes produce one FlowFile per message. Fragmentation is the price of not
     * losing fields; a reader with an explicit schema avoids it.
     */
    @Test
    public void alternatingSchemasProduceOneRecordSetPerChange() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, MESSAGE_3, MESSAGE_1, MESSAGE_3));

        runner.run(1, true);

        successFlowFiles(4);
    }

    /** A change in the mapped attributes still starts a new record set, schema change or not. */
    @Test
    public void aMappedAttributeChangeStillStartsANewRecordSet() {
        runner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "kind");
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(
                message(MESSAGE_1, "kind", "first"),
                message(MESSAGE_1.replace("\"id\":1", "\"id\":2"), "kind", "first"),
                message(MESSAGE_1.replace("\"id\":1", "\"id\":3"), "kind", "second")));

        runner.run(1, true);

        final List<MockFlowFile> flowFiles = successFlowFiles(2);
        assertEquals("first", flowFiles.get(0).getAttribute("kind"));
        assertEquals("2", flowFiles.get(0).getAttribute("record.count"));
        assertEquals("second", flowFiles.get(1).getAttribute("kind"));
        assertEquals("1", flowFiles.get(1).getAttribute("record.count"));
    }

    /** An unparseable message goes to parse_failure and leaves the open record set as it is. */
    @Test
    public void anUnparseableMessageDoesNotCloseTheRecordSet() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, "this is not json", MESSAGE_1.replace("\"id\":1", "\"id\":2")));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 1);
        assertEquals("this is not json", new String(runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE).get(0).toByteArray(), UTF_8));
        final MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertEquals("2", flowFile.getAttribute("record.count"));
    }

    /**
     * With an explicit schema nothing changes: every message resolves to the same schema, the set stays
     * one FlowFile, and fields outside the schema are dropped by design.
     */
    @Test
    public void anExplicitSchemaIsAppliedUnchanged() {
        runner.setProperty(reader, "Schema Access Strategy", "schema-text-property");
        runner.setProperty(reader, "Schema Text", "{\"type\":\"record\",\"name\":\"event\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"a\",\"type\":[\"null\",\"string\"]}]}");
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, MESSAGE_2, MESSAGE_3));

        runner.run(1, true);

        final MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertEquals("3", flowFile.getAttribute("record.count"));
        // every record resolves to the explicit schema, so the writer emits each payload's own fields verbatim
        assertEquals("[{\"id\":1,\"a\":\"x\"},{\"id\":2,\"a\":\"y\"},{\"id\":3}]", new String(flowFile.toByteArray(), UTF_8));
    }

    private List<MockFlowFile> successFlowFiles(final int expected) {
        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, expected);
        return runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
    }

    private static List<Message<GenericRecord>> messages(final String... payloads) {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();
        for (final String payload : payloads) {
            msgs.add(message(payload, null, null));
        }
        return msgs;
    }

    @SafeVarargs
    private static List<Message<GenericRecord>> messages(final Message<GenericRecord>... msgs) {
        final List<Message<GenericRecord>> list = new ArrayList<>();
        Collections.addAll(list, msgs);
        return list;
    }

    private static int nextId = 1;

    private static Message<GenericRecord> message(final String payload, final String propertyName, final String propertyValue) {
        final Map<String, String> properties = propertyName == null ? null : Collections.singletonMap(propertyName, propertyValue);
        return new MockPulsarMessage<GenericRecord>(TOPIC, payload.getBytes(UTF_8), "1234:" + (nextId++) + ":0", properties, null);
    }
}
