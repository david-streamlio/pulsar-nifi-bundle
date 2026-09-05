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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * The ordering key is a separate concern from the message key: the message key routes to a partition and
 * drives compaction, the ordering key decides Key_Shared dispatch and takes precedence there when set. Until
 * #196 the publishers could only set the message key, so the two were forced to be the same value.
 * <p>
 * <i>Ordering Key</i> on {@code PublishPulsar} and <i>Ordering Key Field</i> on {@code PublishPulsarRecord}
 * mirror the <i>Message Key</i> / <i>Message Key Field</i> pair. Unset, nothing is put on the message and
 * Pulsar's own fallback to the message key applies, so no existing flow changes.
 */
public class PublishPulsarOrderingKeyTest extends AbstractPulsarProcessorTest<byte[]> {

    private static final String TOPIC = "persistent://public/default/ordering";

    private void publishPulsarRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    private void publishPulsarRecordRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);

        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("tenant", RecordFieldType.STRING);
        reader.addSchemaField("session", RecordFieldType.STRING);
        reader.addSchemaField("reading", RecordFieldType.INT);
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);

        final MockRecordWriter writer = new MockRecordWriter("tenant, session, reading");
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    private TypedMessageBuilder<byte[]> builder() {
        return mockClientService.getMockTypedMessageBuilder();
    }

    private List<String> orderingKeysSent(final int expectedMessages) {
        final ArgumentCaptor<byte[]> orderingKeys = ArgumentCaptor.forClass(byte[].class);
        verify(builder(), times(expectedMessages)).orderingKey(orderingKeys.capture());
        return orderingKeys.getAllValues().stream().map(bytes -> new String(bytes, UTF_8)).toList();
    }

    // --- PublishPulsar -------------------------------------------------------------------------------------

    @Test
    public void publishPulsarPutsTheOrderingKeyOnTheMessage() throws Exception {
        publishPulsarRunner();
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_KEY, "tenant-a");
        runner.setProperty(AbstractPulsarProducerProcessor.ORDERING_KEY, "session-7");

        runner.enqueue("payload".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);
        verify(builder(), times(1)).key("tenant-a");
        assertEquals(List.of("session-7"), orderingKeysSent(1));
    }

    @Test
    public void publishPulsarEvaluatesTheOrderingKeyAgainstFlowFileAttributes() throws Exception {
        publishPulsarRunner();
        runner.setProperty(AbstractPulsarProducerProcessor.ORDERING_KEY, "${session.id}");

        runner.enqueue("payload".getBytes(UTF_8), Map.of("session.id", "s-42"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);
        assertEquals(List.of("s-42"), orderingKeysSent(1));
    }

    /** Every demarcated message of a FlowFile carries the FlowFile's ordering key, as it does the message key. */
    @Test
    public void publishPulsarAppliesTheOrderingKeyToEveryDemarcatedMessage() throws Exception {
        publishPulsarRunner();
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarProducerProcessor.ORDERING_KEY, "session-7");

        runner.enqueue("one\ntwo\nthree".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);
        assertEquals(Arrays.asList("session-7", "session-7", "session-7"), orderingKeysSent(3));
    }

    /** Unset is the existing behaviour: nothing is set, and Pulsar falls back to the message key on its own. */
    @Test
    public void publishPulsarSetsNoOrderingKeyWhenThePropertyIsUnset() throws Exception {
        publishPulsarRunner();
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_KEY, "tenant-a");

        runner.enqueue("payload".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);
        verify(builder(), never()).orderingKey(any());
    }

    /** An attribute the expression does not find is not an ordering key. */
    @Test
    public void publishPulsarSetsNoOrderingKeyWhenTheExpressionIsEmpty() throws Exception {
        publishPulsarRunner();
        runner.setProperty(AbstractPulsarProducerProcessor.ORDERING_KEY, "${missing.attribute}");

        runner.enqueue("payload".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);
        verify(builder(), never()).orderingKey(any());
    }

    // --- PublishPulsarRecord -------------------------------------------------------------------------------

    @Test
    public void publishPulsarRecordTakesTheOrderingKeyFromTheNamedField() throws Exception {
        publishPulsarRecordRunner();
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "tenant");
        runner.setProperty(PublishPulsarRecord.ORDERING_KEY_FIELD, "session");

        runner.enqueue("acme,s-1,10\nacme,s-2,11\nglobex,s-1,12".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        verify(builder(), times(2)).key("acme");
        verify(builder(), times(1)).key("globex");
        assertEquals(Arrays.asList("s-1", "s-2", "s-1"), orderingKeysSent(3));
    }

    /** A record whose field is null gets no ordering key; the other records of the FlowFile still do. */
    @Test
    public void publishPulsarRecordSkipsTheOrderingKeyForARecordWithoutTheField() throws Exception {
        publishPulsarRecordRunner();
        runner.setProperty(PublishPulsarRecord.ORDERING_KEY_FIELD, "session");

        runner.enqueue("acme,s-1,10\nacme,,11".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        assertEquals(List.of("s-1"), orderingKeysSent(1));
    }

    @Test
    public void publishPulsarRecordSetsNoOrderingKeyWhenTheFieldIsNotConfigured() throws Exception {
        publishPulsarRecordRunner();
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "tenant");

        runner.enqueue("acme,s-1,10".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        verify(builder(), never()).orderingKey(any());
    }
}
