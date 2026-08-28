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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.commons.io.IOUtils;

@SeeAlso({PublishPulsar.class, ConsumePulsarRecord.class, PublishPulsarRecord.class})
@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar. The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "message.count", description = "The number of messages received from Pulsar"),
    @WritesAttribute(attribute = MessageBatchAttributes.MESSAGE_ID_ATTRIBUTE,
        description = "The unique identifier of the Pulsar message. Only set when the FlowFile contains exactly one message"),
    @WritesAttribute(attribute = MessageBatchAttributes.FIRST_MESSAGE_ID_ATTRIBUTE,
        description = "The identifier of the first Pulsar message written to the FlowFile"),
    @WritesAttribute(attribute = MessageBatchAttributes.LAST_MESSAGE_ID_ATTRIBUTE,
        description = "The identifier of the last Pulsar message written to the FlowFile"),
    @WritesAttribute(attribute = "pulsar.property.*", description = "The properties of the Pulsar message(s), prefixed with 'pulsar.property.'. "
        + "When the FlowFile contains several messages, only the properties whose value is identical in every message are set")
})
public class ConsumePulsar extends AbstractPulsarConsumerProcessor<byte[]> {

    public static final String MSG_COUNT = "message.count";

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            Consumer<GenericRecord> consumer = getConsumer(context, getConsumerId(context, session.get()));

            if (consumer == null) {
                context.yield();
                return;
            }

            if (context.getProperty(ASYNC_ENABLED).asBoolean()) {
                consumeAsync(consumer, context, session);
                handleAsync(consumer, context, session);
            } else {
                consume(consumer, context, session);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }
    }

    private void handleAsync(final Consumer<GenericRecord> consumer, ProcessContext context, ProcessSession session) {
        try {
            Future<List<Message<GenericRecord>>> done = getConsumerService().poll(5, TimeUnit.SECONDS);

            if (done != null) {

                final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                    .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;

                // Cumulative acks are NOT permitted on Shared subscriptions.
                final boolean shared = isSharedSubscription(context);
                
                List<Message<GenericRecord>> messages = done.get();

                if (CollectionUtils.isNotEmpty(messages)) {
                    FlowFile flowFile = null;
                    OutputStream out = null;
                    AtomicInteger msgCount = new AtomicInteger(0);

                    Map<String, String> lastAttributes = null;
                    Message<GenericRecord> lastMessage = null;
                    Map<String, String> currentAttributes = null;
                    MessageBatchAttributes batchAttributes = null;

                    // acknowledged only once the session carrying their content has committed
                    final List<Message<GenericRecord>> pendingAcks = new ArrayList<>();

                    for (Message<GenericRecord> msg : messages) {
                        currentAttributes = getMappedFlowFileAttributes(context, msg);

                       if (lastAttributes != null && !lastAttributes.equals(currentAttributes)) {
                            // mapped attributes changed, write the current flowfile and start a new one
                            IOUtils.closeQuietly(out);

                            if (msgCount.get() < 1) {
                                // every message in this batch had an empty payload, so there is nothing to
                                // route; the synchronous path has always discarded these
                                session.remove(flowFile);
                            } else {
                                flowFile = session.putAllAttributes(flowFile, batchAttributes.toAttributes());
                                flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                                session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                                session.transfer(flowFile, REL_SUCCESS);
                            }

                            acknowledgeOnCommit(session, consumer, pendingAcks, shared, true);

                            lastAttributes = null;
                            lastMessage = null;
                        }

                        if (lastAttributes == null) {
                            flowFile = session.create();
                            flowFile = session.putAllAttributes(flowFile, currentAttributes);
                            batchAttributes = new MessageBatchAttributes();

                            out = session.write(flowFile);
                            msgCount.set(0);
                        }

                        lastAttributes = currentAttributes;
                        lastMessage = msg;
                        batchAttributes.add(msg);
 
                        try {
                            byte[] data = msg.getData();

                            if (data != null && data.length > 0) {
                                // only write demarcators between messages that carry content: writing one
                                // before checking the payload put a stray separator in the FlowFile for
                                // every empty message, which reads downstream as a blank record
                                if (msgCount.get() > 0) {
                                    out.write(demarcatorBytes);
                                }

                                out.write(data);
                                msgCount.getAndIncrement();
                            }

                            // content is in the FlowFile now, so this message may be acknowledged once the
                            // session commits - never before
                            pendingAcks.add(msg);

                        } catch (final IOException ioEx) {
                            getLogger().error("Unable to write the message to a FlowFile", ioEx);
                            // roll back WITHOUT acknowledging, so the broker redelivers
                            pendingAcks.clear();
                            session.rollback();
                            return;
                        }
                    }

                    IOUtils.closeQuietly(out);

                    if (msgCount.get() < 1) {
                        session.remove(flowFile);
                    } else {
                        flowFile = session.putAllAttributes(flowFile, batchAttributes.toAttributes());
                        flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                        session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                        session.transfer(flowFile, REL_SUCCESS);
                    }

                    acknowledgeOnCommit(session, consumer, pendingAcks, shared, true);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        } finally {
            drainAcknowledgments();
        }
    }

    /**
     * Acknowledges the given messages once - and only once - the session carrying their content has
     * committed, then clears the list.
     * <p>
     * Acknowledging earlier loses messages. A message acknowledged before its content is written, or after
     * the session has been rolled back, is gone from Pulsar without ever having reached a FlowFile: the
     * broker will not redeliver it and nothing routes it to a failure relationship. Deferring to the commit
     * callback keeps the processor at-least-once - a failure after the write means redelivery, which is
     * duplication rather than loss.
     *
     * @param session the session carrying the messages' content
     * @param consumer the consumer the messages came from
     * @param pending the messages to acknowledge; emptied before returning
     * @param shared whether the subscription is Shared, where cumulative acknowledgement is not permitted
     */
    private void acknowledgeOnCommit(final ProcessSession session, final Consumer<GenericRecord> consumer,
                                     final List<Message<GenericRecord>> pending, final boolean shared) {
        acknowledgeOnCommit(session, consumer, pending, shared, false);
    }

    private void acknowledgeOnCommit(final ProcessSession session, final Consumer<GenericRecord> consumer,
                                     final List<Message<GenericRecord>> pending, final boolean shared,
                                     final boolean async) {

        if (pending.isEmpty()) {
            session.commitAsync();
            return;
        }

        final List<Message<GenericRecord>> toAcknowledge = new ArrayList<>(pending);
        pending.clear();

        session.commitAsync(() -> {
            if (async) {
                getAckService().submit(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        acknowledgeAll(consumer, toAcknowledge, shared, true);
                        return null;
                    }
                });
            } else {
                acknowledgeAll(consumer, toAcknowledge, shared, false);
            }
        });
    }

    /**
     * Acknowledges a batch whose content is already safely in NiFi, using the asynchronous client calls
     * when the processor is running in async mode so the behaviour matches how it consumed.
     */
    private void acknowledgeAll(final Consumer<GenericRecord> consumer,
                                final List<Message<GenericRecord>> toAcknowledge, final boolean shared,
                                final boolean async) {
        try {
            if (shared) {
                // cumulative acknowledgement is not permitted on Shared subscriptions
                for (final Message<GenericRecord> message : toAcknowledge) {
                    if (async) {
                        consumer.acknowledgeAsync(message).get();
                    } else {
                        consumer.acknowledge(message);
                    }
                }
            } else {
                final Message<GenericRecord> last = toAcknowledge.get(toAcknowledge.size() - 1);

                if (async) {
                    consumer.acknowledgeCumulativeAsync(last).get();
                } else {
                    consumer.acknowledgeCumulative(last);
                }
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            getLogger().error("Interrupted while acknowledging {} consumed message(s); they will be redelivered",
                    new Object[] {toAcknowledge.size()}, interrupted);
        } catch (final PulsarClientException | ExecutionException ackEx) {
            // The content is safely in NiFi; the messages will simply be redelivered.
            getLogger().error("Unable to acknowledge {} consumed message(s); they will be redelivered",
                    new Object[] {toAcknowledge.size()}, ackEx);
        }
    }

    private void consume(Consumer<GenericRecord> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
 
        try {
            final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                    .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

            final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                    .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;
            
            // Cumulative acks are NOT permitted on Shared subscriptions.
            final boolean shared = isSharedSubscription(context);

            FlowFile flowFile = null;
            OutputStream out = null;
            Message<GenericRecord> msg = null;
            Message<GenericRecord> lastMsg = null;
            AtomicInteger msgCount = new AtomicInteger(0);
            AtomicInteger loopCounter = new AtomicInteger(0);

            Map<String, String> lastAttributes = null;
            Map<String, String> currentAttributes = null;
            MessageBatchAttributes batchAttributes = null;

            // Messages whose content is already in the FlowFile being built. They are acknowledged only
            // once the session that carries them has committed - see acknowledgeConsumed().
            final List<Message<GenericRecord>> pendingAcks = new ArrayList<>();

            while (loopCounter.get() < maxMessages && (msg = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                currentAttributes = getMappedFlowFileAttributes(context, msg);

                if (lastMsg != null && !lastAttributes.equals(currentAttributes)) {
                    IOUtils.closeQuietly(out);

                    if (msgCount.get() < 1) {
                        session.remove(flowFile);
                    } else {
                        flowFile = session.putAllAttributes(flowFile, batchAttributes.toAttributes());
                        flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                        session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                        session.transfer(flowFile, REL_SUCCESS);
                        getLogger().debug("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                            new Object[]{flowFile, msgCount.toString()});
                    }

                    acknowledgeOnCommit(session, consumer, pendingAcks, shared);

                    lastAttributes = null;
                    lastMsg = null;
                }

                if (lastMsg == null) {
                    flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, currentAttributes);
                    batchAttributes = new MessageBatchAttributes();

                    out = session.write(flowFile);
                    msgCount.set(0);
                }

                try {
                    lastMsg = msg;
                    lastAttributes = currentAttributes;
                    batchAttributes.add(msg);
                    loopCounter.incrementAndGet();

                    byte[] data = msg.getData();

                    if (data != null && data.length > 0) {
                        // only write demarcators between messages that carry content
                        if (msgCount.get() > 0) {
                            out.write(demarcatorBytes);
                        }

                        out.write(data);
                        msgCount.getAndIncrement();
                    }

                    // Taken responsibility for this message: its content is in the FlowFile, or it had no
                    // content to write. Acknowledging happens after the session commits, not here.
                    pendingAcks.add(msg);

                } catch (final IOException ioEx) {
                    getLogger().error("Unable to create flow file ", ioEx);
                    // Roll back WITHOUT acknowledging. This used to acknowledge cumulatively immediately
                    // after the rollback, which discarded the FlowFiles and told the broker the messages
                    // were handled: they were neither in NiFi nor redeliverable from Pulsar.
                    pendingAcks.clear();
                    session.rollback();
                    return;
                }
            }
            
            IOUtils.closeQuietly(out);

            if (msgCount.get() < 1) {
                if (flowFile != null) {
                    session.remove(flowFile);
                }
            } else {
                flowFile = session.putAllAttributes(flowFile, batchAttributes.toAttributes());
                flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().debug("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                   new Object[]{flowFile, msgCount.toString()});
            }

            acknowledgeOnCommit(session, consumer, pendingAcks, shared);

        } catch (PulsarClientException e) {
            getLogger().error("Error communicating with Apache Pulsar", e);
            context.yield();
            session.rollback();
        }
    }
}
