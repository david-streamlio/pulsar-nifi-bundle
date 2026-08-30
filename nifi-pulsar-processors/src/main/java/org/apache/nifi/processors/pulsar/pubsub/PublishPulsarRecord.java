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

import java.util.*;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.pulsar.utils.KeyValueTopicSchema;
import org.apache.nifi.processors.pulsar.utils.PublishPulsarUtils;
import org.apache.nifi.processors.pulsar.utils.PublisherLease;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

@Tags({"Apache", "Pulsar", "Record", "csv", "json", "avro", "logs", "Put", "Send", "Message", "PubSub", "1.0"})
@CapabilityDescription("Sends the contents of a FlowFile as individual records to Apache Pulsar using the Pulsar 1.x client API. "
    + "The contents of the FlowFile are expected to be record-oriented data that can be read by the configured Record Reader. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsarRecord.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")
@SeeAlso({PublishPulsar.class, ConsumePulsar.class, ConsumePulsarRecord.class})
@TriggerWhenEmpty
@SupportsBatching
public class PublishPulsarRecord extends AbstractPulsarProducerProcessor<byte[]> {

    static final AllowableValue SCHEMA_FROM_RECORD_WRITER = new AllowableValue("Record Writer", "Record Writer",
            "Serialize each record with the configured Record Writer. The bytes are whatever that writer "
            + "emits, which is only accepted by a topic that has no schema of its own.");

    static final AllowableValue SCHEMA_FROM_TOPIC = new AllowableValue("Topic Schema", "Topic Schema",
            "Encode each record with the schema the topic currently carries, so a schema-aware consumer can "
            + "decode it. Falls back to the Record Writer when the topic has no Avro or JSON schema.");

    public static final PropertyDescriptor MESSAGE_SCHEMA_STRATEGY = new PropertyDescriptor.Builder()
            .name("MESSAGE_SCHEMA_STRATEGY")
            .displayName("Message Schema Strategy")
            .description("How records are serialized into Pulsar messages. Publishing to a topic that "
                    + "carries a schema requires 'Topic Schema': the broker validates every message against "
                    + "the topic's schema, so output from the Record Writer is rejected unless it happens to "
                    + "match. Topics without a schema accept either.")
            .required(false)
            .allowableValues(SCHEMA_FROM_RECORD_WRITER, SCHEMA_FROM_TOPIC)
            .defaultValue(SCHEMA_FROM_RECORD_WRITER.getValue())
            .build();

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

    public static final PropertyDescriptor KEY_VALUE_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("KEY_VALUE_KEY_FIELD")
            .displayName("KeyValue Key Field")
            .description("The record field holding the key when publishing to a topic whose schema is a "
                    + "KeyValue schema. Only used with the 'Topic Schema' strategy. On a SEPARATED topic "
                    + "this field becomes the message key, so Message Key Field must not also be set.")
            .required(false)
            .defaultValue(KeyValueTopicSchema.DEFAULT_KEY_FIELD)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor KEY_VALUE_VALUE_FIELD = new PropertyDescriptor.Builder()
            .name("KEY_VALUE_VALUE_FIELD")
            .displayName("KeyValue Value Field")
            .description("The record field holding the value when publishing to a topic whose schema is a "
                    + "KeyValue schema. Only used with the 'Topic Schema' strategy.")
            .required(false)
            .defaultValue(KeyValueTopicSchema.DEFAULT_VALUE_FIELD)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MESSAGE_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("message-key-field")
            .displayName("Message Key Field")
            .description("The name of a field in the Input Records that should be used as the Key for the Pulsar message.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(MESSAGE_SCHEMA_STRATEGY);
        properties.add(MESSAGE_KEY_FIELD);
        properties.add(KEY_VALUE_KEY_FIELD);
        properties.add(KEY_VALUE_VALUE_FIELD);
        properties.addAll(AbstractPulsarProducerProcessor.PROPERTIES);
        PROPERTIES = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final List<FlowFile> flowFiles = PublishPulsarUtils.pollFlowFiles(session);

        if (flowFiles.isEmpty()) {
            // Because we TriggerWhenEmpty, the framework can give us many more threads that we actually need,
            // so yield when there is no work to do.
            context.yield();
            return;
        }

        final Iterator<FlowFile> itr = flowFiles.iterator();

        while (itr.hasNext()) {
            final FlowFile flowFile = itr.next();
            final String topicName = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
            final boolean asyncFlag = (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean());

            PublisherLease lease = getPublisherPool().obtainPublisher(topicName);

            if (lease == null) {
                getLogger().error("Unable to publish to topic {}", new Object[] {topicName});
                session.transfer(flowFile, REL_FAILURE);
            } else {
                final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                        .asControllerService(RecordReaderFactory.class);

                final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                        .asControllerService(RecordSetWriterFactory.class);

                final String messageKeyField = context.getProperty(MESSAGE_KEY_FIELD)
                        .evaluateAttributeExpressions(flowFile).getValue();

                final boolean useTopicSchema = SCHEMA_FROM_TOPIC.getValue()
                        .equals(context.getProperty(MESSAGE_SCHEMA_STRATEGY).getValue());

                try {
                    // leases are pooled and reused across FlowFiles, so count only the sends made for this FlowFile
                    final long sentBefore = lease.complete();

                    session.read(flowFile, in -> {
                        try {
                            final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
                            final RecordSet recordSet = reader.createRecordSet();

                            final RecordSchema schema = writerFactory.getSchema(flowFile.getAttributes(), recordSet.getSchema());
                            lease.publish(flowFile, recordSet, writerFactory, schema, messageKeyField,
                                    getMappedMessageProperties(context, flowFile), asyncFlag, useTopicSchema,
                                    context.getProperty(KEY_VALUE_KEY_FIELD).evaluateAttributeExpressions().getValue(),
                                    context.getProperty(KEY_VALUE_VALUE_FIELD).evaluateAttributeExpressions().getValue());

                        } catch (final SchemaNotFoundException | MalformedRecordException e) {
                            throw new ProcessException(e);
                        }
                    });

                    long messagesSent = lease.complete() - sentBefore;
                    session.putAttribute(flowFile, MSG_COUNT, Long.toString(messagesSent));
                    session.putAttribute(flowFile, TOPIC_NAME, topicName);
                    session.getProvenanceReporter().send(flowFile,
                            getPulsarClientService().getPulsarBrokerRootURL(),
                            String.format("Sent %d records", messagesSent));

                    session.transfer(flowFile, REL_SUCCESS);

                } catch (final Exception ex) {
                    getLogger().error("Unable to process session due to ", ex);
                    session.transfer(flowFile, REL_FAILURE);
                } finally {
                    // return the producer to the pool (or close it, if the pool has already been closed)
                    lease.close();
                }
            }
        }

    }

}
