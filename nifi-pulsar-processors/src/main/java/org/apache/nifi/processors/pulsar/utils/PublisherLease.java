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

import org.apache.commons.compress.utils.IOUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.avro.generic.GenericDatumWriter;
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

    /** The topic's Avro schema, parsed once and re-parsed only if the definition itself changes. */
    private String cachedSchemaDefinition;
    private org.apache.avro.Schema cachedAvroSchema;

    public PublisherLease(Producer producer, ComponentLog logger) {
        this(producer, logger, null);
    }

    public PublisherLease(Producer producer, ComponentLog logger, Schema<byte[]> topicSchema) {
        this.producer = producer;
        this.logger = logger;
        this.topicSchema = topicSchema;
    }

    /**
     * The Avro schema the topic currently carries, or null when it has none, when the producer was not
     * created with a schema-discovering one, or when the schema is not Avro.
     * <p>
     * Producers are created with {@code Schema.AUTO_PRODUCE_BYTES()}, which binds to the topic when the
     * producer is created and then reports the topic's registered SchemaInfo.
     */
    org.apache.avro.Schema getTopicAvroSchema() {
        if (topicSchema == null) {
            return null;
        }

        try {
            final SchemaInfo info = topicSchema.getSchemaInfo();

            if (info == null || info.getType() != SchemaType.AVRO) {
                cachedSchemaDefinition = null;
                cachedAvroSchema = null;
                return null;
            }

            final String definition = new String(info.getSchema(), StandardCharsets.UTF_8);

            // Leases are pooled and serve many FlowFiles, so parsing this on every publish rebuilds the
            // same Avro type tree over and over. Keyed on the definition rather than cached outright, so a
            // schema that does change is still picked up rather than frozen at whatever was seen first.
            if (!definition.equals(cachedSchemaDefinition)) {
                cachedAvroSchema = new org.apache.avro.Schema.Parser().parse(definition);
                cachedSchemaDefinition = definition;
            }

            return cachedAvroSchema;
        } catch (final RuntimeException e) {
            // getSchemaInfo() throws when the schema was never bound to a topic
            logger.debug("Unable to determine the topic's schema; falling back to the configured writer", e);
            return null;
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
     *                       schema, so a topic without one behaves exactly as before.
     */
    public void publish(final FlowFile flowFile, final RecordSet recordSet, final RecordSetWriterFactory writerFactory,
                        final RecordSchema schema, final String messageKeyField, Map<String, String> messageProperties,
                        boolean async, boolean useTopicSchema) throws IOException {

        final org.apache.avro.Schema avroSchema = useTopicSchema ? getTopicAvroSchema() : null;

        if (useTopicSchema && avroSchema == null) {
            logger.debug("The topic carries no Avro schema; encoding with the configured record writer instead");
        }

        // Built once for the whole record set rather than per record: both are reusable across writes on
        // the same schema, and the buffer below is already reset each iteration.
        final GenericDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter =
                avroSchema == null ? null : new GenericDatumWriter<>(avroSchema);
        BinaryEncoder encoder = null;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();

        try {
            while ((record = recordSet.next()) != null) {
                baos.reset();

                final byte[] messageContent;
                final String messageKey;

                if (avroSchema != null) {
                    encoder = EncoderFactory.get().binaryEncoder(baos, encoder);
                    encodeWithTopicSchema(record, avroSchema, datumWriter, encoder);
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

    public long complete() {
        return this.messagesSent.get();
    }
    protected CompletableFuture<MessageId> sendAsync(Producer producer, String key, Map<String, String> properties, byte[] value) {
        TypedMessageBuilder tmb = producer.newMessage().properties(properties).value(value);

        if (key != null) {
            tmb = tmb.key(key);
        }
        return tmb.sendAsync();
    }

    protected CompletableFuture<MessageId> send(Producer producer, String key, Map<String, String> properties, byte[] value) {
        return CompletableFuture.supplyAsync(() -> {
            TypedMessageBuilder tmb = producer.newMessage().properties(properties).value(value);

            if (key != null) {
                tmb = tmb.key(key);
            }
            try {
                return tmb.send();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
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
