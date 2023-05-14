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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.MessageTuple;
import org.apache.nifi.stream.io.util.StreamDemarcator;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

@SeeAlso({ConsumePulsar.class, ConsumePulsarRecord.class, PublishPulsarRecord.class})
@Tags({"Apache", "Pulsar", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Pulsar using the Pulsar Producer API."
    + "The messages to send may be individual FlowFiles or may be delimited, using a user-specified delimiter, such as "
    + "a new-line. The complementary NiFi processor for fetching messages is ConsumePulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "This attribute is added only to FlowFiles that are routed to success.")
@TriggerWhenEmpty
public class PublishPulsar extends AbstractPulsarProducerProcessor<byte[]> {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final Producer<byte[]> producer = getProducer(context, topic);

        /* If we are unable to create a producer, then we know we won't be able
         * to send the message successfully, so go ahead and route to failure now.
         */
        if (producer == null) {
            getLogger().error("Unable to publish to topic {}", new Object[] {topic});
            session.transfer(flowFile, REL_FAILURE);

            if (context.getProperty(ASYNC_ENABLED).asBoolean()) {
                // If we are running in asynchronous mode, then slow down the processor to prevent data loss
                context.yield();
            }
            return;
        }

        final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8) : null;

        if (!context.getProperty(ASYNC_ENABLED).asBoolean()) {
            try {
                send(producer, context, session, flowFile, demarcatorBytes);
            } catch (final PulsarClientException e) {
                getLogger().error("Failed to connect to Pulsar Server due to {}", new Object[]{e});
                session.transfer(flowFile, REL_FAILURE);
            }
        } else {
            byte[] messageContent;

            try (final InputStream in = session.read(flowFile);
                 final StreamDemarcator demarcator = new StreamDemarcator(in, demarcatorBytes, Integer.MAX_VALUE)) {
                List<CompletableFuture<MessageId>> futureList = new ArrayList<>();

                while ((messageContent = demarcator.nextToken()) != null) {
                    futureList.add(sendAsync(producer,
                            getMessageKey(context, flowFile),
                            getMappedMessageProperties(context, flowFile),
                            messageContent));

                }
                demarcator.close();

                // Wait for futures to complete, flush all the producers in parallel etc.
                // Block here until work queue is empty and all producers have been flushed.
                CompletableFuture<MessageId>[] futureArray = futureList.toArray(new CompletableFuture[0]);
                CompletableFuture<Void> allFutures = CompletableFuture.allOf(futureArray);
                allFutures.join(); // wait for all futures to complete
                producer.flush();

                demarcator.close();
                session.transfer(flowFile, REL_SUCCESS);
            } catch (Throwable t) {
                getLogger().error("Unable to process session due to ", t);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    /**
     * Sends the FlowFile content using the demarcator.
     * 
     * @param producer
     * @param context - The current ProcessContext
     * @param session - The current ProcessSession.
     * @param flowFile
     * @param demarcatorBytes - The value used to identify unique records in the list
     * 
     * @throws PulsarClientException
     */
    private void send(Producer<byte[]> producer, ProcessContext context, ProcessSession session, FlowFile flowFile, byte[] demarcatorBytes) throws PulsarClientException {
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger failureCounter = new AtomicInteger(0);
        byte[] messageContent;
        String key = getMessageKey(context, flowFile);
        Map<String, String> properties = getMappedMessageProperties(context, flowFile);

        try (final InputStream in = session.read(flowFile); final StreamDemarcator demarcator = new StreamDemarcator(in, demarcatorBytes, Integer.MAX_VALUE)) {
           while ((messageContent = demarcator.nextToken()) != null) {
              if (send(producer, key, properties, messageContent) != null) {
                 successCounter.incrementAndGet();
              } else {
                 failureCounter.incrementAndGet();
                 break;  // Quit sending messages if we encounter a failure.
              }
            }
        } catch (final IOException ioEx) {
            getLogger().error("Unable to publish message to Pulsar broker " + getPulsarClientService().getPulsarBrokerRootURL(), ioEx);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        /*
         * Record the number of messages that were sent to Apache Pulsar.
         */
        if (successCounter.intValue() > 0) {
            session.adjustCounter("Messages Sent", successCounter.get(), true);
            session.getProvenanceReporter().send(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + producer.getTopic(),
                 "Sent " + successCounter.get() + " messages");
        }

        /* If we had any failures then route the entire FlowFile to Failure.
         * The user will have to take care when re-trying this message to avoid
         * sending duplicate messages.
         */
        if (failureCounter.intValue() == 0) {
           session.transfer(flowFile, REL_SUCCESS);
        } else {
           session.transfer(flowFile, REL_FAILURE);
        }
    }
}
