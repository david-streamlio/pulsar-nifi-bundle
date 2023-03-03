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


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;

@CapabilityDescription("Consumes messages from Apache Pulsar. "
        + "The complementary NiFi processor for sending messages is PublishPulsarRecord. Please note that, at this time, "
        + "the Processor assumes that all records that are retrieved have the same schema. If any of the Pulsar messages "
        + "that are pulled but cannot be parsed or written with the configured Record Reader or Record Writer, the contents "
        + "of the message will be written to a separate FlowFile, and that FlowFile will be transferred to the 'parse.failure' "
        + "relationship. Otherwise, each FlowFile is sent to the 'success' relationship and may contain many individual "
        + "messages within the single FlowFile. A 'record.count' attribute is added to indicate how many messages are contained in the "
        + "FlowFile. No two Pulsar messages will be placed into the same FlowFile if they have different schemas.")
@Tags({"Pulsar", "Get", "Record", "csv", "avro", "json", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@WritesAttributes({
    @WritesAttribute(attribute = "record.count", description = "The number of records received")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SeeAlso({PublishPulsar.class, ConsumePulsar.class, PublishPulsarRecord.class})
public class ConsumePulsarRecord extends AbstractPulsarConsumerProcessor<GenericRecord> {

    public static final String MSG_COUNT = "record.count";
    private static final String RECORD_SEPARATOR = "\n";

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to Pulsar")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a Pulsar consumer to poll a subscription for data "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("2 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse_failure")
            .description("FlowFiles for which the content cannot be parsed.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(MAX_WAIT_TIME);
        properties.addAll(AbstractPulsarConsumerProcessor.PROPERTIES);
        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_PARSE_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

        final byte[] demarcator = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
            .evaluateAttributeExpressions().getValue().getBytes() : RECORD_SEPARATOR.getBytes();

        try {
            Consumer<GenericRecord> consumer = getConsumer(context, getConsumerId(context, session.get()));

            if (consumer == null) { /* If we aren't connected to Pulsar, then just yield */
                context.yield();
                return;
            }

            if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
               consumeAsync(consumer, context, session);
               handleAsync(context, session, consumer, readerFactory, writerFactory, demarcator);
            } else {
               consumeMessages(context, session, consumer, getMessages(consumer, maxMessages), readerFactory, writerFactory, demarcator, false);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }
    }

    /**
     * Retrieve a batch of up to maxMessages for processing.
     *
     * @param consumer - The Pulsar consumer.
     * @param maxMessages - The maximum number of messages to consume from Pulsar.
     * @return A List of Messages
     * @throws PulsarClientException in the event we cannot communicate with the Pulsar broker.
     */
    private List<Message<GenericRecord>> getMessages(final Consumer<GenericRecord> consumer, int maxMessages) throws PulsarClientException {
        final List<Message<GenericRecord>> messages = new LinkedList<Message<GenericRecord>>();
        Message<GenericRecord> msg = null;
        AtomicInteger msgCount = new AtomicInteger(0);

        while (msgCount.get() < maxMessages && (msg = consumer.receive(0, TimeUnit.SECONDS)) != null) {
           messages.add(msg);
           msgCount.incrementAndGet();
        }

        return messages;
    }

    /**
     * Perform the actual processing of the messages, by parsing the messages and writing them out to a FlowFile.
     * All of the messages passed in shall be routed to either SUCCESS or PARSE_FAILURE, allowing us to acknowledge
     * the receipt of the messages to Pulsar, so they are not re-sent.
     *
     * @param context - The current ProcessContext
     * @param session - The current ProcessSession.
     * @param consumer - The Pulsar consumer.
     * @param messages - A list of messages.
     * @param readerFactory - The factory used to read the messages.
     * @param writerFactory - The factory used to write the messages.
     * @param demarcator - The value used to identify unique records in the list
     * @param async - Whether or not to consume the messages asynchronously.
     *  
     * @throws PulsarClientException if there is an issue communicating with Apache Pulsar.
     */
    private void consumeMessages(ProcessContext context, ProcessSession session, 
       final Consumer<GenericRecord> consumer, final List<Message<GenericRecord>> messages,
       final RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory, 
       final byte[] demarcator, final boolean async) throws PulsarClientException {

       if (messages.isEmpty()) {
          return;
       }

       final BlockingQueue<Message<GenericRecord>> parseFailures = 
    	  new LinkedBlockingQueue<Message<GenericRecord>>();
       
       RecordSchema schema = null;
       FlowFile flowFile = null;
       OutputStream rawOut = null;
       RecordSetWriter writer = null;

       Map<String, String> lastAttributes = null;
       Message<GenericRecord> lastMessage = null;
       Map<String, String> currentAttributes = null;

       // Cumulative acks are NOT permitted on Shared subscriptions
       final boolean shared = isSharedSubscription(context);
       
       try {
           for (Message<GenericRecord> msg : messages) {
               currentAttributes = getMappedFlowFileAttributes(context, msg);
               
               // if the current message's mapped attribute values differ from the previous set's,
               // write out the active record set and clear various references so that we'll start a new one
               if (lastAttributes != null && !lastAttributes.equals(currentAttributes)) {
                   final WriteResult result;
                   try {
                       result = writer.finishRecordSet();
                   } finally {
                       writer.close();
                   }
                   closeOutputStream(rawOut);

                   if (result != WriteResult.EMPTY) {
                       flowFile = session.putAllAttributes(flowFile, result.getAttributes());
                       flowFile = session.putAttribute(flowFile, MSG_COUNT, result.getRecordCount() + "");
                       session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                       session.transfer(flowFile, REL_SUCCESS);
                   } else {
                       session.rollback();
                   }

                   handleFailures(session, parseFailures, demarcator);
                   parseFailures.clear();
                   
                   if (!shared) {
                	 acknowledgeCumulative(consumer, lastMessage, async);
                   }

                   lastAttributes = null;
                   lastMessage = null;
               }

               // if there's no record set actively being written, begin one
               byte[] data = msg.getData();
               if (lastMessage == null) {
                   flowFile = session.create();
                   flowFile = session.putAllAttributes(flowFile, currentAttributes);
                   schema = getSchema(flowFile, readerFactory, data);
                   rawOut = session.write(flowFile);
                   writer = getRecordWriter(writerFactory, schema, rawOut, flowFile);

                   if (schema == null || writer == null) {
                       parseFailures.add(msg);
                       session.remove(flowFile);
                       closeOutputStream(rawOut);
                       getLogger().error("Unable to create a record writer to consume from the Pulsar topic");
                       continue;
                   }

                   writer.beginRecordSet();
               }

               lastAttributes = currentAttributes;
               lastMessage = msg;

               if (shared) {
            	 acknowledge(consumer, msg, async);
               }
               
               // write each of the records in the current message to the active record set. These will each
               // have the same mapped flowfile attribute values, which means that it's ok that they are all placed
               // in the same output flowfile.
               
               final InputStream in = new ByteArrayInputStream(data);
               try {
                   RecordReader r = readerFactory.createRecordReader(flowFile, in, getLogger());
                   for (Record record = r.nextRecord(); record != null; record = r.nextRecord()) {
                       writer.write(record);
                   }
               } catch (MalformedRecordException | IOException | SchemaNotFoundException e) {
                   parseFailures.add(msg);
               }
           }

           if (writer != null) {
               final WriteResult result;
               try {
                   result = writer.finishRecordSet();
               } finally {
                   writer.close();
               }
               closeOutputStream(rawOut);

               if (result != WriteResult.EMPTY) {
                   flowFile = session.putAllAttributes(flowFile, result.getAttributes());
                   flowFile = session.putAttribute(flowFile, MSG_COUNT, result.getRecordCount() + "");
                   session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                   session.transfer(flowFile, REL_SUCCESS);
               } else {
                   // We were able to parse the records, but unable to write them to the FlowFile
                   session.rollback();
               }
           }
       } catch (IOException e) {
           getLogger().error("Unable to consume from Pulsar topic ", e);
       }

       handleFailures(session, parseFailures, demarcator);
       
       if (!shared) {
    	  acknowledgeCumulative(consumer, messages.get(messages.size() - 1), async);
       }
    }

    private void acknowledge(final Consumer<GenericRecord> consumer, final Message<GenericRecord> msg, final boolean async) throws PulsarClientException {
    	if (async) {
    		getAckService().submit(new Callable<Object>() {
    			@Override
    			public Object call() throws Exception {
    				return consumer.acknowledgeAsync(msg).get();
    			}
    		});
    	}
    	else {
    		consumer.acknowledge(msg);;
    	}
    }
    
    private void acknowledgeCumulative(final Consumer<GenericRecord> consumer, final Message<GenericRecord> msg, final boolean async) throws PulsarClientException {
    	if (async) {
    		getAckService().submit(new Callable<Object>() {
    			@Override
    			public Object call() throws Exception {
    				return consumer.acknowledgeCumulativeAsync(msg).get();
    			}
    		});
    	}
    	else {
    		consumer.acknowledgeCumulative(msg);
    	}
    }
    
    private void handleFailures(ProcessSession session, 
    	BlockingQueue<Message<GenericRecord>> parseFailures, byte[] demarcator) {

        if (parseFailures.isEmpty()) {
           return;
        }

        FlowFile flowFile = session.create();
        OutputStream rawOut = session.write(flowFile);

        try {
           Iterator<Message<GenericRecord>> failureIterator = parseFailures.iterator();
           
           for (int idx = 0; failureIterator.hasNext(); idx++) {
        	  Message<GenericRecord> msg = failureIterator.next();

              if (msg != null && msg.getData() != null) {
            	 if (idx > 0) {
            		 rawOut.write(demarcator);
            	 }
            	 
                 rawOut.write(msg.getData());
              }
           }
            closeOutputStream(rawOut);
           session.transfer(flowFile, REL_PARSE_FAILURE);
        } catch (IOException e) {
           getLogger().error("Unable to route failures", e);
        }
    }

    /**
     * Pull messages off of the CompletableFuture's held in the consumerService and process them in a batch.
     * 
     * @param context - The current ProcessContext
     * @param session - The current ProcessSession.
     * @param consumer - The Pulsar consumer.
     * @param readerFactory - The factory used to read the messages.
     * @param writerFactory - The factory used to write the messages.
     * @param demarcator - The bytes used to demarcate the individual messages.
     * 
     * @throws PulsarClientException if there is an issue connecting to the Pulsar cluster. 
     */
    protected void handleAsync(ProcessContext context, ProcessSession session, final Consumer<GenericRecord> consumer,
         final RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory, byte[] demarcator) throws PulsarClientException {

        final Integer queryTimeout = context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();

        try {
             Future<List<Message<GenericRecord>>> done = null;
             do {
                 done = getConsumerService().poll(queryTimeout, TimeUnit.SECONDS);

                 if (done != null) {
                    List<Message<GenericRecord>> messages = done.get();
                    if (!messages.isEmpty()) {
                      consumeMessages(context, session, consumer, messages, readerFactory, writerFactory, demarcator, true);
                    }
                 }
             } while (done != null);

        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        }
    }

    private RecordSchema getSchema(FlowFile flowFile, RecordReaderFactory readerFactory, byte[] msgValue) {
        try (InputStream in = new ByteArrayInputStream(msgValue)) {
            return readerFactory.createRecordReader(flowFile, in, getLogger()).getSchema();
        } catch (MalformedRecordException | IOException | SchemaNotFoundException e){
            getLogger().error("Unable to determine the schema", e);
            return null;
        }
    }

    private RecordSetWriter getRecordWriter(RecordSetWriterFactory writerFactory, 
    	RecordSchema srcSchema, OutputStream out, FlowFile flowFile) {
        try {
            RecordSchema writeSchema = writerFactory.getSchema(Collections.emptyMap(), srcSchema);
            return writerFactory.createWriter(getLogger(), writeSchema, out, flowFile);
        } catch (SchemaNotFoundException | IOException e) {
           return null;
        }
    }
}
