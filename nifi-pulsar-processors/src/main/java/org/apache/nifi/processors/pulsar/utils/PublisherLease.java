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
package org.apache.nifi.processors.pulsar.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.StreamDemarcator;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class PublisherLease implements Closeable {

    private final ComponentLog logger;
    private final Producer producer;

    private final AtomicLong messagesSent = new AtomicLong(0L);

    /** The schema the producer was created with, used to discover what schema the topic carries. */
    private final Schema<byte[]> topicSchema;

    /** The topic's schema, parsed once and re-parsed only if the definition itself changes. */
    /** Holds the parsed key and value schemas of a KeyValue topic between records. */
    private final KeyValueTopicSchema keyValueTopicSchema = new KeyValueTopicSchema();

    private String cachedSchemaDefinition;
    private TopicSchema cachedTopicSchema;

    public PublisherLease(Producer producer, ComponentLog logger) {
        this(producer, logger, null);
    }

    public PublisherLease(Producer producer, ComponentLog logger, Schema<byte[]> topicSchema) {
        this.producer = producer;
        this.logger = logger;
        this.topicSchema = topicSchema;
    }

    /**
     * The schema the topic currently carries, or null when it has none, when the producer was not created
     * with a schema-discovering one, or when the schema is of a type records cannot be encoded with.
     * <p>
     * Producers are created with {@code Schema.AUTO_PRODUCE_BYTES()}, which binds to the topic when the
     * producer is created and then reports the topic's registered SchemaInfo.
     */
    /**
     * The topic's schema type, or null when it has none. Kept separate from {@link #getTopicSchema()},
     * which only answers for the two struct types that map to an Avro document; a primitive topic (#189)
     * has a type but no record shape.
     */
    /** The topic's SchemaInfo as the client reports it, or null when the topic has none. */
    SchemaInfo rawTopicSchemaInfo() {
        if (topicSchema == null) {
            return null;
        }

        try {
            return topicSchema.getSchemaInfo();
        } catch (final RuntimeException e) {
            return null;
        }
    }

    SchemaType getTopicSchemaType() {
        if (topicSchema == null) {
            return null;
        }

        try {
            final SchemaInfo info = topicSchema.getSchemaInfo();
            return info == null ? null : info.getType();
        } catch (final RuntimeException e) {
            // getSchemaInfo() throws when the schema was never bound to a topic
            return null;
        }
    }

    TopicSchema getTopicSchema() {
        if (topicSchema == null) {
            return null;
        }

        try {
            final SchemaInfo info = topicSchema.getSchemaInfo();

            if (info == null || (info.getType() != SchemaType.AVRO && info.getType() != SchemaType.JSON)) {
                cachedSchemaDefinition = null;
                cachedTopicSchema = null;
                return null;
            }

            final String definition = new String(info.getSchema(), StandardCharsets.UTF_8);

            // Leases are pooled and serve many FlowFiles, so parsing this on every publish rebuilds the
            // same Avro type tree over and over. Keyed on the definition rather than cached outright, so a
            // schema that does change is still picked up rather than frozen at whatever was seen first.
            if (cachedTopicSchema == null || !definition.equals(cachedSchemaDefinition)
                    || cachedTopicSchema.getType() != info.getType()) {
                cachedTopicSchema =
                        new TopicSchema(new org.apache.avro.Schema.Parser().parse(definition), info.getType());
                cachedSchemaDefinition = definition;
            }

            return cachedTopicSchema;
        } catch (final RuntimeException e) {
            // getSchemaInfo() throws when the schema was never bound to a topic
            logger.debug("Unable to determine the topic's schema; falling back to the configured writer", e);
            return null;
        }
    }

    /**
     * A schema a topic carries, in the form records get encoded against.
     * <p>
     * Pulsar registers AVRO and JSON schemas the same way - the schema definition is an Avro schema
     * document describing the record in both cases - and only the {@link SchemaType} says which encoding
     * goes on the wire: Avro binary for one, plain JSON for the other. So the two travel together; the
     * definition alone cannot say how to encode.
     */
    static final class TopicSchema {

        private final org.apache.avro.Schema definition;
        private final SchemaType type;

        TopicSchema(final org.apache.avro.Schema definition, final SchemaType type) {
            this.definition = definition;
            this.type = type;
        }

        org.apache.avro.Schema getDefinition() {
            return definition;
        }

        SchemaType getType() {
            return type;
        }

        boolean isJson() {
            return type == SchemaType.JSON;
        }
    }

    public void publish(final FlowFile flowFile, final InputStream flowFileContent, final String messageKey,
                        Map<String, String> messageProperties, final byte[] demarcatorBytes, boolean async) throws IOException {

        byte[] messageContent;
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();

        if (demarcatorBytes == null || demarcatorBytes.length == 0) {
            messageContent = new byte[(int) flowFile.getSize()];
            StreamUtils.fillBuffer(flowFileContent, messageContent);
            futureList.add(async ?
                    sendAsync(producer, messageKey, messageProperties, messageContent) :
                    send(producer, messageKey, messageProperties, messageContent));

        } else {
            try (final StreamDemarcator demarcator = new StreamDemarcator(flowFileContent, demarcatorBytes, Integer.MAX_VALUE)) {

                while ((messageContent = demarcator.nextToken()) != null) {
                    futureList.add(async ?
                            sendAsync(producer, messageKey, messageProperties, messageContent) :
                            send(producer, messageKey, messageProperties, messageContent));

                    if (futureList.size() > 99) {
                        producer.flush();
                        awaitAll(futureList);
                    }
                }
            }
        }

        // Block here until every message has been confirmed by the broker.
        awaitAll(futureList);

        IOUtils.closeQuietly(flowFileContent);
    }

    public void publish(final FlowFile flowFile, final RecordSet recordSet, final RecordSetWriterFactory writerFactory,
                        final RecordSchema schema, final String messageKeyField, Map<String, String> messageProperties,
                        boolean async) throws IOException {
        publish(flowFile, recordSet, writerFactory, schema, messageKeyField, messageProperties, async, false);
    }

    /**
     * @param useTopicSchema encode each record with the schema the topic carries rather than with the
     *                       configured record writer. Falls back to the writer when the topic has no Avro
     *                       or JSON schema, so a topic without one behaves exactly as before.
     */
    public void publish(final FlowFile flowFile, final RecordSet recordSet, final RecordSetWriterFactory writerFactory,
                        final RecordSchema schema, final String messageKeyField, Map<String, String> messageProperties,
                        boolean async, boolean useTopicSchema) throws IOException {
        publish(flowFile, recordSet, writerFactory, schema, messageKeyField, messageProperties, async, useTopicSchema,
                KeyValueTopicSchema.DEFAULT_KEY_FIELD, KeyValueTopicSchema.DEFAULT_VALUE_FIELD);
    }

    /**
     * @param keyValueKeyField the record field holding the key of a KeyValue topic
     * @param keyValueValueField the record field holding its value
     */
    public void publish(final FlowFile flowFile, final RecordSet recordSet, final RecordSetWriterFactory writerFactory,
                        final RecordSchema schema, final String messageKeyField, Map<String, String> messageProperties,
                        boolean async, boolean useTopicSchema, final String keyValueKeyField,
                        final String keyValueValueField) throws IOException {

        final TopicSchema resolvedSchema = useTopicSchema ? getTopicSchema() : null;

        if (useTopicSchema && resolvedSchema == null) {
            logger.debug("The topic carries no Avro or JSON schema; encoding with the configured record writer instead");
        }

        // A primitive topic takes the record's single field as its whole payload, so it is neither an Avro
        // encode nor a Record Writer serialization.
        final SchemaType primitiveType = useTopicSchema && resolvedSchema == null
                && PrimitiveTopicSchema.supports(getTopicSchemaType()) ? getTopicSchemaType() : null;

        // A KeyValue topic carries two schemas; the key goes either inside the payload (INLINE) or into
        // the message's key metadata (SEPARATED), so it is neither of the paths below.
        final SchemaInfo keyValueSchema = useTopicSchema && resolvedSchema == null
                && KeyValueTopicSchema.supports(rawTopicSchemaInfo()) ? rawTopicSchemaInfo() : null;

        final org.apache.avro.Schema avroSchema = resolvedSchema == null ? null : resolvedSchema.getDefinition();
        final boolean encodeAsJson = resolvedSchema != null && resolvedSchema.isJson();

        // Built once for the whole record set rather than per record: all of these are reusable across
        // writes on the same schema, and the buffer below is already reset each iteration.
        final GenericDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter =
                (avroSchema == null || encodeAsJson) ? null : new GenericDatumWriter<>(avroSchema);
        // AUTO_CLOSE_TARGET off: the stream below outlives each generator and is reused for every record.
        final JsonFactory jsonFactory =
                encodeAsJson ? new JsonFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET) : null;
        BinaryEncoder encoder = null;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();

        try {
            while ((record = recordSet.next()) != null) {
                baos.reset();

                final byte[] messageContent;
                final String messageKey;

                if (keyValueSchema != null) {
                    final KeyValueTopicSchema.EncodedKeyValue encoded =
                            keyValueTopicSchema.encode(record, keyValueSchema, keyValueKeyField, keyValueValueField);
                    messageContent = encoded.getPayload();

                    if (encoded.getMessageKey() != null) {
                        // The schema owns the message key on a SEPARATED topic, so Message Key Field cannot
                        // also own it. Refusing beats silently overwriting one with the other; the topic's
                        // schema is not knowable at validation time, so this has to be caught here.
                        if (messageKeyField != null && !messageKeyField.isEmpty()) {
                            throw new IOException("The topic's KeyValue schema is SEPARATED, so its key field "
                                    + "'" + keyValueKeyField + "' becomes the message key; remove Message Key "
                                    + "Field, which would overwrite it");
                        }

                        futureList.add(async
                                ? sendAsyncWithKeyBytes(producer, encoded.getMessageKey(), messageProperties, messageContent)
                                : sendWithKeyBytes(producer, encoded.getMessageKey(), messageProperties, messageContent));

                        if (futureList.size() > 100) {
                            producer.flush();
                            awaitAll(futureList);
                        }
                        continue;
                    }
                } else if (primitiveType != null) {
                    messageContent = encodeWithPrimitiveTopicSchema(record, primitiveType);
                } else if (avroSchema != null) {
                    if (encodeAsJson) {
                        encodeWithTopicJsonSchema(record, avroSchema, jsonFactory, baos);
                    } else {
                        encoder = EncoderFactory.get().binaryEncoder(baos, encoder);
                        encodeWithTopicSchema(record, avroSchema, datumWriter, encoder);
                    }
                    messageContent = baos.toByteArray();
                } else {
                    try (final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos, flowFile)) {
                        writer.write(record);
                        writer.flush();
                    }

                    messageContent = baos.toByteArray();
                }
                messageKey = getMessageKey(flowFile, writerFactory, record.getValue(messageKeyField));

                futureList.add(async ?
                        sendAsync(producer, messageKey, messageProperties, messageContent) :
                        send(producer, messageKey, messageProperties, messageContent));

                if (futureList.size() > 100) {
                    producer.flush();
                    awaitAll(futureList);
                }
            }

            awaitAll(futureList);

        } catch (final Exception ex) {
            logger.error("Unable to Publish Pulsar Records", ex);
            throw new IOException(ex.getCause());
        }

    }

    /**
     * Waits for every send in {@code futures} to be confirmed by the broker, then empties the list.
     * <p>
     * This used to be {@code futures.stream().map(future -> future.get())} with no terminal operation.
     * Streams are lazy, so the mapping function never ran: the futures were never waited on and the list
     * was cleared regardless. Only the final partial batch was ever awaited, which meant that for any
     * FlowFile larger than one batch the sends were fire and forget - a failure was swallowed with no log
     * and no failure relationship, and the message count reported success for messages that had not been
     * confirmed (and in the worst case never arrived).
     *
     * @param futures the sends issued since the last await; emptied before returning
     */
    private void awaitAll(final List<CompletableFuture<MessageId>> futures) {
        if (futures.isEmpty()) {
            return;
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            // only count what the broker actually confirmed
            messagesSent.addAndGet(futures.size());
        } finally {
            futures.clear();
        }
    }

    /**
     * Encodes a NiFi record as Avro binary using the topic's own schema.
     * <p>
     * This is the encoding Pulsar's AVRO schema uses, so the broker accepts it and a schema-aware consumer
     * decodes it. Writing the record with the configured record writer instead produced whatever that
     * writer emits - JSON, CSV - which the broker now rejects and which a consumer could never decode.
     *
     * @param record the record to encode
     * @param avroSchema the schema the topic carries
     * @return the encoded message content
     * @throws IOException if the record cannot be converted or encoded
     */
    /**
     * Encodes a record for a topic whose schema is a single primitive value. The record must have exactly
     * one field: a primitive topic carries one value per message, so a record with several fields has no
     * unambiguous mapping onto it, and guessing which field was meant would publish the wrong data
     * silently. Failing here routes the FlowFile to failure with a message naming the fields instead.
     */
    private byte[] encodeWithPrimitiveTopicSchema(final Record record, final SchemaType primitiveType)
            throws IOException {
        final List<String> fields = record.getSchema().getFieldNames();

        if (fields.size() != 1) {
            throw new IOException("A " + primitiveType + " topic carries a single value per message, but the "
                    + "record has " + fields.size() + " fields " + fields + "; publish a single-field record "
                    + "or use a topic whose schema is a record");
        }

        final String fieldName = fields.get(0);
        return PrimitiveTopicSchema.encode(primitiveType, record.getValue(fieldName), fieldName);
    }

    private void encodeWithTopicSchema(final Record record, final org.apache.avro.Schema avroSchema,
                                       final GenericDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter,
                                       final BinaryEncoder encoder) throws IOException {

        final org.apache.avro.generic.GenericRecord avroRecord;

        try {
            avroRecord = AvroTypeUtil.createAvroRecord(record, avroSchema);
        } catch (final Exception e) {
            throw new IOException("Unable to convert the record to the topic's schema " + avroSchema.getFullName(), e);
        }

        datumWriter.write(avroRecord, encoder);
        encoder.flush();
    }

    /**
     * Encodes a NiFi record as plain JSON conforming to the topic's schema.
     * <p>
     * A Pulsar JSON schema is registered as an Avro schema document, but what it puts on the wire is
     * ordinary JSON - Jackson's rendering of the message object - and not Avro's own JSON encoding, which
     * wraps a union value in its branch name. So the record is converted to the topic's schema exactly as
     * the Avro path converts it, and then written out as plain JSON.
     * <p>
     * Falling through to the configured record writer instead emitted whatever that writer emits - CSV,
     * say - and, unlike an AVRO topic, a JSON one accepts it: the message landed on the topic looking
     * valid and a schema-aware consumer decoded every one of its fields as null.
     *
     * @param record the record to encode
     * @param avroSchema the schema document the topic carries
     * @param jsonFactory the factory to build the generator from, shared across the record set
     * @param out the buffer to encode into
     * @throws IOException if the record cannot be converted or encoded
     */
    private void encodeWithTopicJsonSchema(final Record record, final org.apache.avro.Schema avroSchema,
                                           final JsonFactory jsonFactory, final ByteArrayOutputStream out)
            throws IOException {

        final org.apache.avro.generic.GenericRecord avroRecord;

        try {
            avroRecord = AvroTypeUtil.createAvroRecord(record, avroSchema);
        } catch (final Exception e) {
            throw new IOException("Unable to convert the record to the topic's schema " + avroSchema.getFullName(), e);
        }

        try (JsonGenerator generator = jsonFactory.createGenerator(out)) {
            writeAsJson(generator, avroSchema, avroRecord);
        }
    }

    /**
     * Writes one value as plain JSON, guided by the schema branch it was converted to.
     * <p>
     * Deliberately not Avro's {@code JsonEncoder}: that emits Avro's JSON encoding, in which a union value
     * appears as {@code {"string": "x"}} and which a Pulsar consumer reading the same JSON schema cannot
     * decode. Here a union writes the value it holds, and nothing else, which is what Pulsar's own JSON
     * schema produces and reads back.
     *
     * @param generator the generator to write to
     * @param schema the schema of this value
     * @param value the value, already converted to the schema by {@link AvroTypeUtil}
     */
    private static void writeAsJson(final JsonGenerator generator, final org.apache.avro.Schema schema,
                                    final Object value) throws IOException {

        if (value == null) {
            generator.writeNull();
            return;
        }

        switch (schema.getType()) {
            case UNION:
                writeAsJson(generator, schema.getTypes().get(GenericData.get().resolveUnion(schema, value)), value);
                break;

            case RECORD:
                final org.apache.avro.generic.GenericRecord nested = (org.apache.avro.generic.GenericRecord) value;
                generator.writeStartObject();
                for (final org.apache.avro.Schema.Field field : schema.getFields()) {
                    generator.writeFieldName(field.name());
                    writeAsJson(generator, field.schema(), nested.get(field.pos()));
                }
                generator.writeEndObject();
                break;

            case ARRAY:
                generator.writeStartArray();
                if (value instanceof Object[]) {
                    for (final Object element : (Object[]) value) {
                        writeAsJson(generator, schema.getElementType(), element);
                    }
                } else {
                    for (final Object element : (Iterable<?>) value) {
                        writeAsJson(generator, schema.getElementType(), element);
                    }
                }
                generator.writeEndArray();
                break;

            case MAP:
                generator.writeStartObject();
                for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    generator.writeFieldName(String.valueOf(entry.getKey()));
                    writeAsJson(generator, schema.getValueType(), entry.getValue());
                }
                generator.writeEndObject();
                break;

            case BYTES:
                final ByteBuffer buffer = ((ByteBuffer) value).duplicate();
                final byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                generator.writeBinary(bytes);
                break;

            case FIXED:
                generator.writeBinary(((GenericFixed) value).bytes());
                break;

            case BOOLEAN:
                generator.writeBoolean((Boolean) value);
                break;

            case INT:
                generator.writeNumber(((Number) value).intValue());
                break;

            case LONG:
                generator.writeNumber(((Number) value).longValue());
                break;

            case FLOAT:
                generator.writeNumber(((Number) value).floatValue());
                break;

            case DOUBLE:
                generator.writeNumber(((Number) value).doubleValue());
                break;

            case NULL:
                generator.writeNull();
                break;

            // STRING and ENUM, plus anything a future Avro adds: Avro hands strings back as Utf8, so the
            // value is rendered rather than cast.
            default:
                generator.writeString(value.toString());
                break;
        }
    }

    public long complete() {
        return this.messagesSent.get();
    }
    /** A KeyValue SEPARATED key is the encoded key itself, so it goes on as bytes rather than a string. */
    protected CompletableFuture<MessageId> sendAsyncWithKeyBytes(Producer producer, byte[] keyBytes, Map<String, String> properties, byte[] value) {
        return producer.newMessage().properties(properties).keyBytes(keyBytes).value(value).sendAsync();
    }

    protected CompletableFuture<MessageId> sendWithKeyBytes(Producer producer, byte[] keyBytes, Map<String, String> properties, byte[] value)
            throws PulsarClientException {
        try {
            return CompletableFuture.completedFuture(
                    producer.newMessage().properties(properties).keyBytes(keyBytes).value(value).send());
        } catch (final PulsarClientException e) {
            final CompletableFuture<MessageId> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    protected CompletableFuture<MessageId> sendAsync(Producer producer, String key, Map<String, String> properties, byte[] value) {
        TypedMessageBuilder tmb = producer.newMessage().properties(properties).value(value);

        if (key != null) {
            tmb = tmb.key(key);
        }
        return tmb.sendAsync();
    }

    /**
     * Sends one message and waits for it, on the calling thread.
     * <p>
     * This used to wrap the blocking send in {@code CompletableFuture.supplyAsync()}, handing every message
     * to the common ForkJoinPool. The sends then raced, so a FlowFile's messages reached the broker in
     * whatever order the pool happened to run them - a FlowFile holding sensor-2 then sensor-3 arrived as
     * sensor-3, sensor-2. Pulsar preserves ordering per producer and flows rely on that, and out-of-order
     * sends also defeat message keys, whose whole purpose is to order the messages sharing one.
     * <p>
     * Sending on the calling thread costs the pipelining that dispatching bought. Asynchronous mode is the
     * place to ask for that: {@link #sendAsync} issues {@code sendAsync()} in record order on this thread,
     * which Pulsar pipelines while preserving the order the calls were made in.
     */
    protected CompletableFuture<MessageId> send(Producer producer, String key, Map<String, String> properties, byte[] value) {
        TypedMessageBuilder tmb = producer.newMessage().properties(properties).value(value);

        if (key != null) {
            tmb = tmb.key(key);
        }

        try {
            return CompletableFuture.completedFuture(tmb.send());
        } catch (final PulsarClientException e) {
            final CompletableFuture<MessageId> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    private String getMessageKey(final FlowFile flowFile, final RecordSetWriterFactory writerFactory,
                                 final Object keyValue) throws IOException, SchemaNotFoundException {
        final byte[] messageKey;
        if (keyValue == null) {
            messageKey = null;
        } else if (keyValue instanceof byte[]) {
            messageKey = (byte[]) keyValue;
        } else if (keyValue instanceof Byte[]) {
            // This case exists because in our Record API we currently don't have a BYTES type, we use an Array of type
            // Byte, which creates a Byte[] instead of a byte[]. We should address this in the future, but we should
            // account for the log here.
            final Byte[] bytes = (Byte[]) keyValue;
            final byte[] bytesPrimitive = new byte[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                bytesPrimitive[i] = bytes[i];
            }
            messageKey = bytesPrimitive;
        } else if (keyValue instanceof Record) {
            final Record keyRecord = (Record) keyValue;
            try (final ByteArrayOutputStream os = new ByteArrayOutputStream(1024)) {
                try (final RecordSetWriter writerKey = writerFactory.createWriter(logger, keyRecord.getSchema(), os, flowFile)) {
                    writerKey.write(keyRecord);
                    writerKey.flush();
                }
                messageKey = os.toByteArray();
            }
        } else {
            final String keyString = keyValue.toString();
            messageKey = keyString.getBytes(StandardCharsets.UTF_8);
        }
        return (messageKey == null) ? null : new String(messageKey);
    }

    @Override
    public void close() {
        // Always attempt to close the producer, even if flushing fails, to avoid leaking
        // the underlying producer/connection resources when flush() throws.
        try {
            producer.flush();
        } catch (final PulsarClientException pcEx) {
            logger.error("Unable to close producer", pcEx);
        } finally {
            try {
                producer.close();
            } catch (final PulsarClientException pcEx) {
                logger.error("Unable to close producer", pcEx);
            }
        }
    }

    /**
     * Get the topic name for this producer lease
     * @return the topic name that this producer is publishing to
     */
    public String getTopicName() {
        return producer != null ? producer.getTopic() : null;
    }
}
