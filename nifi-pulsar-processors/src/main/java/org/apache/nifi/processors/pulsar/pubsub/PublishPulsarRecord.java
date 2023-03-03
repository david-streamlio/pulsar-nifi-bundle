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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.MessageTuple;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.pulsar.client.api.Producer;

@Tags({"Apache", "Pulsar", "Record", "csv", "json", "avro", "logs", "Put", "Send", "Message", "PubSub", "1.0"})
@CapabilityDescription("Sends the contents of a FlowFile as individual records to Apache Pulsar using the Pulsar 1.x client API. "
    + "The contents of the FlowFile are expected to be record-oriented data that can be read by the configured Record Reader. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsarRecord.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")
@SeeAlso({PublishPulsar.class, ConsumePulsar.class, ConsumePulsarRecord.class})
@TriggerWhenEmpty
public class PublishPulsarRecord extends AbstractPulsarProducerProcessor<byte[]> {

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("RECORD_READER")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("RECORD_WRITER")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to Pulsar")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.addAll(AbstractPulsarProducerProcessor.PROPERTIES);
        PROPERTIES = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        handleFailures(session);

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
            getLogger().error("Unable to publish to topic {}", topic);
            session.transfer(flowFile, REL_FAILURE);

            if (context.getProperty(ASYNC_ENABLED).asBoolean()) {
                // If we are running in asynchronous mode, then slow down the processor to prevent data loss
                context.yield();
            }
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        final Map<String, String> attributes = flowFile.getAttributes();
        final AtomicLong messagesSent = new AtomicLong(0L);
        final InputStream in = session.read(flowFile);
        try {
            final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
            final RecordSet recordSet = reader.createRecordSet();
            final RecordSchema schema = writerFactory.getSchema(attributes, recordSet.getSchema());
            final String key = getMessageKey(context, flowFile);
            final Map<String, String> properties = getMappedMessageProperties(context, flowFile);
            final boolean asyncFlag = (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean());

            try {
                messagesSent.addAndGet(send(producer, writerFactory, schema, reader, topic, key, properties, asyncFlag));
                closeInputStream(in);
                session.putAttribute(flowFile, MSG_COUNT, messagesSent.get() + "");
                session.putAttribute(flowFile, TOPIC_NAME, topic);
                session.adjustCounter("Messages Sent", messagesSent.get(), true);
                session.getProvenanceReporter().send(flowFile, getPulsarClientService().getPulsarBrokerRootURL(), "Sent " + messagesSent.get() + " records");
                session.transfer(flowFile, REL_SUCCESS);
            } catch (InterruptedException e) {
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (final SchemaNotFoundException | MalformedRecordException | IOException e) {
            closeInputStream(in);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private int send(
            final Producer<byte[]> producer,
            final RecordSetWriterFactory writerFactory,
            final RecordSchema schema,
            final RecordReader reader,
            String topic, String key, Map<String, String> properties, boolean asyncFlag) throws IOException, SchemaNotFoundException, InterruptedException {

        final RecordSet recordSet = reader.createRecordSet();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        Record record;
        int recordCount = 0;

        try {
            while ((record = recordSet.next()) != null) {
                recordCount++;
                baos.reset();

                try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, baos, Collections.emptyMap())) {
                    writer.write(record);
                    writer.flush();
                }
                if (asyncFlag) {
                   workQueue.put(new MessageTuple<>(topic, key, properties, baos.toByteArray()));
                } else {
                   send(producer, key, properties, baos.toByteArray());
                }
            }
            return recordCount;
        } finally {
            reader.close();
        }
    }
}
