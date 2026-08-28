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

    public PublisherLease(Producer producer, ComponentLog logger) {
        this.producer = producer;
        this.logger = logger;
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

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();

        try {
            while ((record = recordSet.next()) != null) {
                baos.reset();

                final byte[] messageContent;
                final String messageKey;

                // final Map<String, String> additionalAttributes;
                try (final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos, flowFile)) {
                    final WriteResult writeResult = writer.write(record);
                    // additionalAttributes = writeResult.getAttributes();
                    writer.flush();
                }

                messageContent = baos.toByteArray();
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
     * Clears the message count so a pooled lease does not carry one FlowFile's total into the next.
     */
    void reset() {
        this.messagesSent.set(0L);
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
