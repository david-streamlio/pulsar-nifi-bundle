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
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;

@SeeAlso({PublishPulsar.class, ConsumePulsarRecord.class, PublishPulsarRecord.class})
@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar. The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "message.count", description = "The number of messages received from Pulsar")
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

                if (!messages.isEmpty()) {
                    FlowFile flowFile = null;
                    OutputStream out = null;
                    AtomicInteger msgCount = new AtomicInteger(0);

                    Map<String, String> lastAttributes = null;
                    Message<GenericRecord> lastMessage = null;
                    Map<String, String> currentAttributes = null;

                    for (Message<GenericRecord> msg : messages) {
                        currentAttributes = getMappedFlowFileAttributes(context, msg);

                       if (lastAttributes != null && !lastAttributes.equals(currentAttributes)) {
                            // mapped attributes changed, write the current flowfile and start a new one
                           closeOutputStream(out);

                            flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                            session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                            session.transfer(flowFile, REL_SUCCESS);
                            session.commitAsync();

                            if (!shared) {
	                            final Message<GenericRecord> finalMessage = lastMessage;
	                            // Cumulatively acknowledge consuming the messages for non-shared subs

	                            getAckService().submit(() -> consumer.acknowledgeCumulativeAsync(finalMessage).get());
                            }

                            lastAttributes = null;
                            lastMessage = null;
                        }

                        if (lastAttributes == null) {
                            flowFile = session.create();
                            flowFile = session.putAllAttributes(flowFile, currentAttributes);

                            out = session.write(flowFile);
                            msgCount.set(0);
                        }

                        lastAttributes = currentAttributes;
                        lastMessage = msg;

                        if (shared) {
                        	// acknowledge each message individually for shared subs
                        	getAckService().submit(() -> consumer.acknowledgeAsync(msg).get());
                        }

                        try {
                        	//only write demarcators between messages
                        	if (demarcatorBytes != null && msgCount.get() > 0) {
                        		out.write(demarcatorBytes);
                        	}

                        	 byte[] data = msg.getData();

                             if (data != null && data.length > 0) {
                               out.write(data);
                               msgCount.getAndIncrement();
                             }

                        } catch (final IOException ioEx) {
                            session.rollback();
                            return;
                        }
                    }

                    closeOutputStream(out);

                    flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                    session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                    session.transfer(flowFile, REL_SUCCESS);
                    session.commitAsync();
                }
                // Cumulatively acknowledge consuming the message for non-shared subs
                if (!shared) {
                    getAckService().submit(() -> consumer.acknowledgeCumulativeAsync(messages.get(messages.size() - 1)).get());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
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

            while (loopCounter.get() < maxMessages && (msg = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                currentAttributes = getMappedFlowFileAttributes(context, msg);

                if (lastMsg != null && !lastAttributes.equals(currentAttributes)) {
                    closeOutputStream(out);

                    if (!shared)  {
                        consumer.acknowledgeCumulative(lastMsg);
                    }

                    if (msgCount.get() < 1) {
                        session.remove(flowFile);
                        session.commitAsync();
                    } else {
                        flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                        session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                        session.transfer(flowFile, REL_SUCCESS);
                        getLogger().debug("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                                flowFile, msgCount.toString());
                    }

                    lastAttributes = null;
                    lastMsg = null;
                }

                if (lastMsg == null) {
                    flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, currentAttributes);

                    out = session.write(flowFile);
                    msgCount.set(0);
                }

                try {
                    lastMsg = msg;
                    lastAttributes = currentAttributes;
                    loopCounter.incrementAndGet();
                    
                    if (shared) {
                    	consumer.acknowledge(msg);
                    }
                    
                    // only write demarcators between messages
                    if (demarcatorBytes != null && msgCount.get() > 0) {
                    	out.write(demarcatorBytes);
                    }
                    
                    byte[] data = msg.getData();
                    
                    if (data != null && data.length > 0) {
                      out.write(data);
                      msgCount.getAndIncrement();
                    }
                    
                } catch (final IOException ioEx) {
                    getLogger().error("Unable to create flow file ", ioEx);
                    session.rollback();
                    if (!shared) {
                        consumer.acknowledgeCumulative(lastMsg);
                    }
  
                    return;
                }
            }

            closeOutputStream(out);

            if (!shared && lastMsg != null)  {
                consumer.acknowledgeCumulative(lastMsg);
            }

            if (msgCount.get() < 1) {
                if (flowFile != null) {
                    session.remove(flowFile);
                    session.commitAsync();
                }
            } else {
                flowFile = session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().debug("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                        flowFile, msgCount.toString());
            }

        } catch (PulsarClientException e) {
            getLogger().error("Error communicating with Apache Pulsar", e);
            context.yield();
            session.rollback();
        }
    }


}
