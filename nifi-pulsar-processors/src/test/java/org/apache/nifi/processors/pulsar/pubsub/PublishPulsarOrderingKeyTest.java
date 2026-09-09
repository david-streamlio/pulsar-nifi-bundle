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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroReader;

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.LogMessage;
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

    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Reading\",\"fields\":["
            + "{\"name\":\"tenant\",\"type\":\"string\"},"
            + "{\"name\":\"session\",\"type\":\"bytes\"},"
            + "{\"name\":\"label\",\"type\":\"string\"},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
            + "{\"name\":\"reading\",\"type\":\"int\"}]}");

    /**
     * An Avro reader, because that is where a {@code Byte[]} comes from: NiFi's {@code AvroTypeUtil} turns an Avro
     * {@code bytes} field into a {@code Byte[]}, not a {@code byte[]}.
     */
    private void publishPulsarRecordRunnerWithAvroInput() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);

        final AvroReader reader = new AvroReader();
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);

        final MockRecordWriter writer = new MockRecordWriter("tenant, session, label, reading");
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    /**
     * An Avro data file with one record per (session, label[, tags]) tuple; every record has the same tenant and
     * reading. The session is a {@code String} (its UTF-8 bytes) or a {@code byte[]} (as given), the tags default
     * to an empty array.
     */
    private static byte[] avroRecords(final Object[]... sessionLabelTags) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(AVRO_SCHEMA))) {
            writer.create(AVRO_SCHEMA, out);
            for (final Object[] tuple : sessionLabelTags) {
                final GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
                record.put("tenant", "acme");
                record.put("session", ByteBuffer.wrap(tuple[0] instanceof byte[] ? (byte[]) tuple[0] : ((String) tuple[0]).getBytes(UTF_8)));
                record.put("label", tuple[1]);
                record.put("tags", tuple.length > 2 ? Arrays.asList((String[]) tuple[2]) : List.of());
                record.put("reading", 1);
                writer.append(record);
            }
        }
        return out.toByteArray();
    }

    private List<byte[]> keyBytesSent(final int expectedMessages) {
        final ArgumentCaptor<byte[]> keys = ArgumentCaptor.forClass(byte[].class);
        verify(builder(), times(expectedMessages)).keyBytes(keys.capture());
        return keys.getAllValues();
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
        runner.setProperty(PublishPulsar.ORDERING_KEY, "session-7");

        runner.enqueue("payload".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 1);
        verify(builder(), times(1)).key("tenant-a");
        assertEquals(List.of("session-7"), orderingKeysSent(1));
    }

    @Test
    public void publishPulsarEvaluatesTheOrderingKeyAgainstFlowFileAttributes() throws Exception {
        publishPulsarRunner();
        runner.setProperty(PublishPulsar.ORDERING_KEY, "${session.id}");

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
        runner.setProperty(PublishPulsar.ORDERING_KEY, "session-7");

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
        runner.setProperty(PublishPulsar.ORDERING_KEY, "${missing.attribute}");

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

    /**
     * The whole point of an ordering key is that every message carrying the same value lands on the same consumer.
     * A boxed byte array - what an Avro {@code bytes} field arrives as - must therefore be unboxed to its content,
     * as the message key is; {@code toString()} on it is an identity hash that differs for every message. And the
     * same field under both properties gives the same bytes: as the ordering key directly, as the message key
     * through {@code keyBytes()}.
     */
    @Test
    public void publishPulsarRecordUnboxesAnAvroBytesFieldUnderBothProperties() throws Exception {
        publishPulsarRecordRunnerWithAvroInput();
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "session");
        runner.setProperty(PublishPulsarRecord.ORDERING_KEY_FIELD, "session");

        runner.enqueue(avroRecords(new Object[] {"s-1", "first"}, new Object[] {"s-1", "second"}, new Object[] {"s-2", "third"}));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        final List<String> messageKeys = keyBytesSent(3).stream().map(bytes -> new String(bytes, UTF_8)).toList();
        assertEquals(Arrays.asList("s-1", "s-1", "s-2"), messageKeys);
        assertEquals("the ordering key must be the field's content, identical for the two s-1 records",
                Arrays.asList("s-1", "s-1", "s-2"), orderingKeysSent(3));
        verify(builder(), never()).key(any());
    }

    /**
     * A binary key travels as bytes, not as text decoded through a charset. Decoding maps every invalid sequence to
     * the same replacement character, so two different byte strings can become one key - here {@code FF FE 01} and
     * {@code FE FF 01}, both "\uFFFD\uFFFD\u0001" under UTF-8 - and on a compacted topic one would supersede the
     * other. With {@code keyBytes()} they stay two keys.
     */
    @Test
    public void aBinaryMessageKeyFieldIsSentAsKeyBytesSoDifferentValuesStayDifferentKeys() throws Exception {
        publishPulsarRecordRunnerWithAvroInput();
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "session");
        final byte[] first = {(byte) 0xFF, (byte) 0xFE, 0x01};
        final byte[] second = {(byte) 0xFE, (byte) 0xFF, 0x01};
        assertEquals("the two keys must be indistinguishable once decoded, or the test proves nothing",
                new String(first, UTF_8), new String(second, UTF_8));

        runner.enqueue(avroRecords(new Object[] {first, "a"}, new Object[] {second, "b"}));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        final List<byte[]> sent = keyBytesSent(2);
        assertArrayEquals(first, sent.get(0));
        assertArrayEquals(second, sent.get(1));
        verify(builder(), never()).key(any());
    }

    /**
     * An empty {@code array<string>} is also an empty {@code Object[]}, and must not be mistaken for an empty byte
     * string: every record whose array is empty would otherwise get the key {@code ""} and they would all
     * supersede each other on a compacted topic.
     */
    @Test
    public void anEmptyArrayFieldIsNotABinaryKey() throws Exception {
        publishPulsarRecordRunnerWithAvroInput();
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "tags");

        runner.enqueue(avroRecords(new Object[] {"s-1", "a", new String[0]}, new Object[] {"s-2", "b", new String[0]}));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        verify(builder(), never()).keyBytes(any());
        verify(builder(), never()).key("");
    }

    /**
     * A field the records do not have yields null for every record, exactly like a field that is present and
     * null - so a misspelt name would set no ordering key and nothing would say so. It is warned about once per
     * FlowFile.
     */
    @Test
    public void aMisspelledOrderingKeyFieldIsWarnedAboutOncePerFlowFile() throws Exception {
        publishPulsarRecordRunner();
        runner.setProperty(PublishPulsarRecord.ORDERING_KEY_FIELD, "sessionId");

        runner.enqueue("acme,s-1,10\nacme,s-2,11\nglobex,s-3,12".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        verify(builder(), never()).orderingKey(any());
        assertEquals(1, runner.getLogger().getWarnMessages().size());
        final LogMessage warning = runner.getLogger().getWarnMessages().get(0);
        final String rendered = String.format(warning.getMsg().replace("{}", "%s"), warning.getArgs());
        assertTrue(rendered, rendered.contains("Ordering Key Field") && rendered.contains("sessionId") && rendered.contains("session"));
    }

    /** The same gap existed for Message Key Field; the same warning closes it. */
    @Test
    public void aMisspelledMessageKeyFieldIsWarnedAboutOncePerFlowFile() throws Exception {
        publishPulsarRecordRunner();
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "tenantId");

        runner.enqueue("acme,s-1,10\nglobex,s-2,11".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        verify(builder(), never()).key(any());
        assertEquals(1, runner.getLogger().getWarnMessages().size());
        final LogMessage warning = runner.getLogger().getWarnMessages().get(0);
        final String rendered = String.format(warning.getMsg().replace("{}", "%s"), warning.getArgs());
        assertTrue(rendered, rendered.contains("Message Key Field") && rendered.contains("tenantId"));
    }

    /** A correctly named field produces no warning, or the warning would be noise on every FlowFile. */
    @Test
    public void aFieldThatExistsProducesNoWarning() throws Exception {
        publishPulsarRecordRunner();
        runner.setProperty(PublishPulsarRecord.MESSAGE_KEY_FIELD, "tenant");
        runner.setProperty(PublishPulsarRecord.ORDERING_KEY_FIELD, "session");

        runner.enqueue("acme,s-1,10".getBytes(UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        assertEquals(0, runner.getLogger().getWarnMessages().size());
    }

    /** Blank means no ordering key on both processors: PublishPulsar's property already said so via isBlank. */
    @Test
    public void publishPulsarRecordTreatsABlankFieldAsNoOrderingKey() throws Exception {
        publishPulsarRecordRunnerWithAvroInput();
        runner.setProperty(PublishPulsarRecord.ORDERING_KEY_FIELD, "label");

        runner.enqueue(avroRecords(new Object[] {"s-1", "   "}, new Object[] {"s-1", "keyed"}));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 1);
        assertEquals(List.of("keyed"), orderingKeysSent(1));
    }

    /**
     * Only PublishPulsar reads <i>Ordering Key</i>; PublishPulsarRecord takes its ordering key per record from
     * <i>Ordering Key Field</i>. A property the processor never reads must not appear in its UI.
     */
    @Test
    public void onlyPublishPulsarOffersTheFlowFileLevelOrderingKey() {
        assertTrue(new PublishPulsar().getPropertyDescriptors().contains(PublishPulsar.ORDERING_KEY));
        assertFalse(new PublishPulsarRecord().getPropertyDescriptors().contains(PublishPulsar.ORDERING_KEY));
        assertTrue(new PublishPulsarRecord().getPropertyDescriptors().contains(PublishPulsarRecord.ORDERING_KEY_FIELD));
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
