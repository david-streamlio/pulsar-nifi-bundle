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
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes;
import org.apache.nifi.processors.pulsar.utils.TopicSchemaRecordDecoder;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

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
        @WritesAttribute(attribute = "record.count", description = "The number of records received"),
        @WritesAttribute(attribute = MessageBatchAttributes.MESSAGE_ID_ATTRIBUTE,
            description = "The unique identifier of the Pulsar message. Only set when the FlowFile was built from exactly one message"),
        @WritesAttribute(attribute = MessageBatchAttributes.FIRST_MESSAGE_ID_ATTRIBUTE,
            description = "The identifier of the first Pulsar message written to the FlowFile"),
        @WritesAttribute(attribute = MessageBatchAttributes.LAST_MESSAGE_ID_ATTRIBUTE,
            description = "The identifier of the last Pulsar message written to the FlowFile"),
        @WritesAttribute(attribute = "pulsar.property.*", description = "The properties of the Pulsar message(s), prefixed with 'pulsar.property.'. "
            + "When the FlowFile contains several messages, only the properties whose value is identical in every message are set")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SeeAlso({PublishPulsar.class, ConsumePulsar.class, PublishPulsarRecord.class})
public class ConsumePulsarRecord extends AbstractPulsarConsumerProcessor<GenericRecord> {

    public static final String MSG_COUNT = "record.count";
    private static final String RECORD_SEPARATOR = "\n";

    static final AllowableValue SCHEMA_FROM_RECORD_READER = new AllowableValue("Record Reader", "Record Reader",
            "Parse each message with the configured Record Reader. The reader has to match how the topic is "
            + "encoded: an AVRO topic carries bare Avro binary and needs an AvroReader whose Schema Text is "
            + "${avro.schema}, while a JSON topic carries text a JsonTreeReader can infer.");

    static final AllowableValue SCHEMA_FROM_TOPIC = new AllowableValue("Topic Schema", "Topic Schema",
            "Build records from the schema the topic carries, which the broker sends with every message. No "
            + "Record Reader is needed and AVRO and JSON topics behave identically. Messages from a topic "
            + "with no schema, or with a schema that has no record shape, fall back to the Record Reader.");

    public static final PropertyDescriptor MESSAGE_SCHEMA_STRATEGY = new PropertyDescriptor.Builder()
            .name("MESSAGE_SCHEMA_STRATEGY")
            .displayName("Message Schema Strategy")
            .description("How Pulsar messages are turned into records. 'Topic Schema' uses the schema "
                    + "registered on the topic, so the encoding is handled for you; 'Record Reader' uses the "
                    + "configured reader, which must match the topic's encoding.")
            .required(false)
            .allowableValues(SCHEMA_FROM_RECORD_READER, SCHEMA_FROM_TOPIC)
            .defaultValue(SCHEMA_FROM_RECORD_READER.getValue())
            .build();

    static final AllowableValue PRIMITIVE_VIA_READER = new AllowableValue("Record Reader if configured",
            "Record Reader if configured",
            "Parse the payload with the Record Reader when one is configured, and wrap it in a single field "
            + "only when there is none. Suits a STRING topic carrying JSON or CSV text.");

    static final AllowableValue PRIMITIVE_AS_RECORD = new AllowableValue("Single-field record",
            "Single-field record",
            "Always wrap the value in a single field, whether a Record Reader is configured or not. Suits a "
            + "topic whose values are genuinely scalar, and leaves the reader free to serve as the fallback "
            + "for topics that have no schema.");

    public static final PropertyDescriptor PRIMITIVE_SCHEMA_HANDLING = new PropertyDescriptor.Builder()
            .name("PRIMITIVE_SCHEMA_HANDLING")
            .displayName("Primitive Schema Handling")
            .description("What to do with a topic whose schema is a primitive. Only used by the 'Topic "
                    + "Schema' strategy. The default defers to the Record Reader when one is configured, "
                    + "which is what a STRING topic carrying JSON text wants; choose 'Single-field record' "
                    + "when the values really are scalar, so that configuring a reader as the schema-less "
                    + "fallback does not change how primitive topics are read.")
            .required(false)
            .allowableValues(PRIMITIVE_VIA_READER, PRIMITIVE_AS_RECORD)
            .defaultValue(PRIMITIVE_VIA_READER.getValue())
            .build();

    public static final PropertyDescriptor PRIMITIVE_VALUE_FIELD = new PropertyDescriptor.Builder()
            .name("PRIMITIVE_VALUE_FIELD")
            .displayName("Primitive Value Field")
            .description("The field name to give the value of a topic whose schema is a primitive - a "
                    + "STRING or INT32 topic, for instance - which carries one value per message and has no "
                    + "fields of its own. Only used by the 'Topic Schema' strategy, and only when no Record "
                    + "Reader is configured: a reader takes precedence, so JSON text on a STRING topic can "
                    + "still be parsed into records rather than wrapped in a single field.")
            .required(false)
            .defaultValue(TopicSchemaRecordDecoder.DEFAULT_PRIMITIVE_FIELD)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles. Required unless Message Schema "
                    + "Strategy is 'Topic Schema', which still falls back to this reader for messages from a "
                    + "topic that has no schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
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

    /**
     * The Record Reader stays required under the default strategy, so an existing configuration that omits
     * it is still invalid for the same reason it always was. Under 'Topic Schema' it is optional, but it is
     * still what messages from a schema-less topic fall back to - without it those go to parse.failure.
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        // Seeded with the superclass results, not an empty list: AbstractPulsarConsumerProcessor enforces
        // that exactly one of Topics / Topics Pattern is set and that Acknowledgment Timeout is at least
        // 10 seconds, and starting empty silently dropped both for this processor (#194).
        final Collection<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        if (!usesTopicSchema(validationContext.getProperty(MESSAGE_SCHEMA_STRATEGY).getValue())
                && !validationContext.getProperty(RECORD_READER).isSet()) {
            results.add(new ValidationResult.Builder()
                    .subject(RECORD_READER.getDisplayName())
                    .valid(false)
                    .explanation("is required unless " + MESSAGE_SCHEMA_STRATEGY.getDisplayName()
                            + " is '" + SCHEMA_FROM_TOPIC.getDisplayName() + "'")
                    .build());
        }

        return results;
    }

    static boolean usesTopicSchema(final String strategy) {
        return SCHEMA_FROM_TOPIC.getValue().equals(strategy);
    }

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MESSAGE_SCHEMA_STRATEGY);
        properties.add(PRIMITIVE_SCHEMA_HANDLING);
        properties.add(PRIMITIVE_VALUE_FIELD);
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
     * @param consumer    - The Pulsar consumer.
     * @param maxMessages - The maximum number of messages to consume from Pulsar.
     * @return A List of Messages
     * @throws PulsarClientException in the event we cannot communicate with the Pulsar broker.
     */
    private List<Message<GenericRecord>> getMessages(final Consumer<GenericRecord> consumer, int maxMessages) throws PulsarClientException {
        List<Message<GenericRecord>> messages = new LinkedList<Message<GenericRecord>>();
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
     * the receipt of the messages to Pulsar, so they are not re-sent. Consecutive messages with the same mapped
     * attributes and the same record schema share one record set; a change in either starts a new one. The acknowledgement is issued once the
     * session carrying the FlowFiles has been committed; if the batch cannot be written, the session is rolled
     * back and nothing is acknowledged, so the broker redelivers the messages instead of losing them.
     *
     * @param context       - The current ProcessContext
     * @param session       - The current ProcessSession.
     * @param consumer      - The Pulsar consumer.
     * @param messages      - A list of messages.
     * @param readerFactory - The factory used to read the messages.
     * @param writerFactory - The factory used to write the messages.
     * @param demarcator    - The value used to identify unique records in the list
     * @param async         - Whether or not to consume the messages asynchronously.
     * @throws PulsarClientException if there is an issue communicating with Apache Pulsar.
     */
    private void consumeMessages(ProcessContext context, ProcessSession session,
                                 final Consumer<GenericRecord> consumer, final List<Message<GenericRecord>> messages,
                                 final RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory,
                                 final byte[] demarcator, final boolean async) throws PulsarClientException {

        if (CollectionUtils.isEmpty(messages)) {
            return;
        }

        // Group by the logical topic so that the partitions of one partitioned topic stay together, and
        // collect into a LinkedHashMap so that the groups - and the messages within them - keep the order
        // in which they were received rather than the hash order of the topic names.
        final List<Message<GenericRecord>> groupedMessages = messages
                .stream()
                .collect(Collectors.groupingBy(msg -> getLogicalTopicName(msg.getTopicName()),
                        LinkedHashMap::new, Collectors.toList()))
                .values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        final BlockingQueue<Message<GenericRecord>> parseFailures =
                new LinkedBlockingQueue<Message<GenericRecord>>();

        RecordSchema schema = null;
        FlowFile flowFile = null;
        OutputStream rawOut = null;
        RecordSetWriter writer = null;

        Map<String, String> lastAttributes = null;
        Message<GenericRecord> lastMessage = null;
        Map<String, String> currentAttributes = null;
        MessageBatchAttributes batchAttributes = null;
        // the messages carried - on success or on parse_failure - by the FlowFiles that have not been
        // committed yet: they are acknowledged once those are, and not at all if they are rolled back
        final List<Message<GenericRecord>> uncommitted = new ArrayList<>();

        // Records that actually reached the record set. The writer's own count is not a reliable stand-in:
        // it counts a record as written before the bytes reach the stream, so a failing content repository
        // still reports a full count for a FlowFile that holds nothing. Counted per record rather than per
        // message, because a message with some good records and one bad one still has content worth routing.
        int writtenRecords = 0;

        // Cumulative acks are NOT permitted on Shared subscriptions
        final boolean shared = isSharedSubscription(context);

        final boolean useTopicSchema = usesTopicSchema(context.getProperty(MESSAGE_SCHEMA_STRATEGY).getValue());

        // Created per batch rather than held on the processor. Concurrent Tasks > 1 gave every task the
        // same decoder, and its schema cache was read and replaced without synchronization, so two tasks
        // decoding different schemas returned each other's records - silently, with no exception, straight
        // to success (#195). One decoder per batch parses each definition once per trigger, which is
        // cheap, and shares nothing.
        final TopicSchemaRecordDecoder topicSchemaDecoder = new TopicSchemaRecordDecoder();
        final String primitiveField = context.getProperty(PRIMITIVE_VALUE_FIELD).evaluateAttributeExpressions().getValue();
        final boolean deferPrimitivesToReader =
                PRIMITIVE_VIA_READER.getValue().equals(context.getProperty(PRIMITIVE_SCHEMA_HANDLING).getValue());

        try {
            for (Message<GenericRecord> msg : groupedMessages) {
                currentAttributes = getMappedFlowFileAttributes(context, msg);
                // Introduce an attribute to distinguish between current and previously captured attributes,
                // particularly when the message originates from a different topic.
                currentAttributes.put("topicName", getLogicalTopicName(msg.getTopicName()));
                // add the schema to the attributes in-case the schema is updated on the topic
                // With AUTO_CONSUME the reader schema is present even for a topic that has no schema at all -
                // it is the AutoConsumeSchema itself - but its SchemaInfo is null. Treat that like no schema.
                final SchemaInfo readerSchemaInfo = msg.getReaderSchema().map(Schema::getSchemaInfo).orElse(null);
                if (readerSchemaInfo != null && readerSchemaInfo.getType() == SchemaType.AVRO) {
                    currentAttributes.put("avro.schema", new String(readerSchemaInfo.getSchema()));
                }

                // Parse the message before deciding which record set it joins. The reader's schema is the
                // message's own - with an inferred schema, the shape of this payload - and a message whose
                // schema differs from the open set's cannot go through the set's writer: a writer only emits
                // the fields of the schema it was created with, so the fields the message does not share
                // would be dropped silently, and a field of a different type would fail to coerce.
                final byte[] data = msg.getData();
                RecordReader reader = null;
                RecordSchema currentSchema = null;
                Record topicSchemaRecord = null;

                // What a primitive topic does is chosen by Primitive Schema Handling rather than inferred
                // from whether a reader happens to be set. A STRING topic carrying JSON text wants the
                // reader; a genuinely scalar topic wants the single field - and because the reader is also
                // the fallback for schema-less topics, configuring one for that reason must not silently
                // decide this too. Deferring to the reader is the default, so nothing changes by upgrading.
                final boolean readerWins = readerFactory != null && deferPrimitivesToReader
                        && TopicSchemaRecordDecoder.isPrimitive(readerSchemaInfo);

                if (useTopicSchema && !readerWins && TopicSchemaRecordDecoder.supports(readerSchemaInfo)) {
                    // The topic's schema decides the record's shape, so no reader is consulted at all. One
                    // message is one record here: a schema-bearing topic carries a single encoded value per
                    // message, unlike a reader, which may find several records in one payload.
                    try {
                        topicSchemaRecord = topicSchemaDecoder.decode(data, readerSchemaInfo, primitiveField);
                        currentSchema = topicSchemaRecord.getSchema();
                    } catch (final IOException | RuntimeException e) {
                        getLogger().debug("Unable to decode a message with the topic's schema", e);
                        topicSchemaRecord = null;
                    }
                } else if (readerFactory != null) {
                    // Either the configured strategy is the Record Reader, or the topic has no schema to
                    // decode with - a schema-less topic, or one whose schema has no record shape.
                    try {
                        reader = readerFactory.createRecordReader(currentAttributes, new ByteArrayInputStream(data), data.length, getLogger());
                        currentSchema = reader.getSchema();
                    } catch (MalformedRecordException | IOException | SchemaNotFoundException e) {
                        IOUtils.closeQuietly(reader);
                        reader = null;
                    }
                }

                // if the current message's mapped attribute values - or its schema - differ from the open
                // record set's, write out the active record set and clear various references so that we'll
                // start a new one. An unparseable message has no schema and leaves the open set as it is.
                if (lastAttributes != null && (!lastAttributes.equals(currentAttributes)
                        || (currentSchema != null && !schema.equals(currentSchema)))) {
                    WriteResult result = writer.finishRecordSet();
                    IOUtils.closeQuietly(writer);
                    IOUtils.closeQuietly(rawOut);
                    writer = null;
                    rawOut = null;

                    if (writtenRecords > 0 && result.getRecordCount() > 0) {
                        flowFile = session.putAllAttributes(flowFile, result.getAttributes());
                        flowFile = session.putAllAttributes(flowFile, batchAttributes.toAttributes());
                        flowFile = session.putAttribute(flowFile, MSG_COUNT, result.getRecordCount() + "");
                        session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                        session.transfer(flowFile, REL_SUCCESS);
                    } else {
                        // the record set holds no records: discard its FlowFile. Rolling the session back
                        // here would also discard everything routed before it in this session.
                        session.remove(flowFile);
                    }

                    dropUnroutedFailures(session, parseFailures, demarcator, uncommitted);
                    commitAndAcknowledge(session, consumer, uncommitted, shared, async);

                    writtenRecords = 0;
                    lastAttributes = null;
                    lastMessage = null;
                }

                // every message ends up in a FlowFile - on success or on parse_failure - or is discarded on
                // purpose, so it is acknowledged with the commit of the FlowFiles it belongs to
                uncommitted.add(msg);

                if (reader == null && topicSchemaRecord == null) {
                    // the message could not be parsed: it is routed to parse_failure with the rest of the set
                    parseFailures.add(msg);
                    continue;
                }

                // if there's no record set actively being written, begin one
                if (lastMessage == null) {
                    flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, currentAttributes);
                    batchAttributes = new MessageBatchAttributes();
                    schema = currentSchema;
                    rawOut = session.write(flowFile);
                    writer = getRecordWriter(writerFactory, schema, rawOut, flowFile);

                    if (writer == null) {
                        parseFailures.add(msg);
                        IOUtils.closeQuietly(reader);
                        // the OutputStream has to be closed before the FlowFile can be removed, otherwise
                        // the session rejects the removal with an IllegalStateException
                        IOUtils.closeQuietly(rawOut);
                        session.remove(flowFile);
                        // no record set is open now: keep the invariant that writer != null means "open"
                        rawOut = null;
                        getLogger().error("Unable to create a record writer to consume from the Pulsar topic");
                        continue;
                    }

                    writer.beginRecordSet();
                }

                lastAttributes = currentAttributes;
                lastMessage = msg;
                batchAttributes.add(msg);

                // write each of the records in the current message to the active record set. These will each
                // have the same mapped flowfile attribute values and the same schema, which means that it's ok
                // that they are all placed in the same output flowfile.
                try {
                    if (topicSchemaRecord != null) {
                        writer.write(topicSchemaRecord);
                        writtenRecords++;
                    } else {
                        for (Record record = reader.nextRecord(); record != null; record = reader.nextRecord()) {
                            writer.write(record);
                            writtenRecords++;
                        }
                    }
                } catch (MalformedRecordException | IOException e) {
                    parseFailures.add(msg);
                } finally {
                    IOUtils.closeQuietly(reader);
                }
            }

            // writer is null when no record set is open: every message in this batch failed to produce a
            // schema or a writer (each one hit the 'continue' above and went to parseFailures), or the last
            // record set was already finished when the mapped attributes changed. There is nothing to
            // finish in that case - the parse failures are routed below.
            WriteResult result = writer == null ? WriteResult.EMPTY : writer.finishRecordSet();

            IOUtils.closeQuietly(writer);
            IOUtils.closeQuietly(rawOut);

            // The record count is what decides this, not identity with WriteResult.EMPTY: a writer that
            // finished with no records returns a result that is not that singleton, and transferring it
            // routes an empty FlowFile to success. That went unnoticed while a write error rolled the whole
            // session back and took the empty FlowFile with it.
            if (writtenRecords > 0 && result.getRecordCount() > 0) {
                flowFile = session.putAllAttributes(flowFile, result.getAttributes());
                flowFile = session.putAllAttributes(flowFile, batchAttributes.toAttributes());
                flowFile = session.putAttribute(flowFile, MSG_COUNT, result.getRecordCount() + "");
                session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                session.transfer(flowFile, REL_SUCCESS);
            } else if (writer != null) {
                // the record set holds no records: discard its FlowFile rather than roll the session back
                session.remove(flowFile);
            }

            dropUnroutedFailures(session, parseFailures, demarcator, uncommitted);
        } catch (IOException e) {
            // nothing has been acknowledged, so the broker redelivers the whole batch
            getLogger().error("Unable to write the received messages to a FlowFile; they will be redelivered", e);
            IOUtils.closeQuietly(writer);
            IOUtils.closeQuietly(rawOut);
            session.rollback();
            return;
        }

        // Commits, then acknowledges: cumulatively for non-shared subscriptions, one message at a time otherwise.
        commitAndAcknowledge(session, consumer, uncommitted, shared, async);
    }

    /**
     * Returns the logical topic a message belongs to.
     * <p>
     * For a partitioned topic Pulsar reports the physical partition in {@link Message#getTopicName()}
     * (<code>persistent://tenant/ns/topic-partition-N</code>). Every partition of a topic shares one schema,
     * so they belong in the same record set; batching on the physical name instead splits each batch into one
     * FlowFile per partition and reorders the messages.
     * <p>
     * Only the partitions of a partitioned topic are rewritten. Anything else - including a non-partitioned
     * topic given as a short name - is returned exactly as the broker reported it, so the <code>topicName</code>
     * attribute is unchanged for the non-partitioned case.
     *
     * @param topic the topic name reported for the message
     * @return the logical (un-partitioned) topic name
     */
    static String getLogicalTopicName(final String topic) {
        if (topic == null || topic.isEmpty()) {
            return topic;
        }

        try {
            final TopicName topicName = TopicName.get(topic);
            return topicName.isPartitioned() ? topicName.getPartitionedTopicName() : topic;
        } catch (final IllegalArgumentException e) {
            // Not a well-formed Pulsar topic name: batch on the raw value rather than failing the flow.
            return topic;
        }
    }

    /**
     * Routes the parse failures and, when their FlowFile could not be written, removes exactly those
     * messages from the set awaiting acknowledgement so the broker redelivers them - and only them.
     *
     * @param session the current session
     * @param parseFailures the messages that could not be parsed; emptied before returning
     * @param demarcator bytes written between messages
     * @param uncommitted the messages awaiting acknowledgement, adjusted in place
     */
    private void dropUnroutedFailures(final ProcessSession session,
                                      final BlockingQueue<Message<GenericRecord>> parseFailures,
                                      final byte[] demarcator,
                                      final List<Message<GenericRecord>> uncommitted) {

        final List<Message<GenericRecord>> failures = new ArrayList<>(parseFailures);

        if (!handleFailures(session, parseFailures, demarcator)) {
            uncommitted.removeAll(failures);
        }

        parseFailures.clear();
    }

    /**
     * Routes the messages that could not be parsed to {@link #REL_PARSE_FAILURE}.
     * <p>
     * Reports whether it succeeded rather than throwing, so that a parse-failure FlowFile which cannot be
     * written costs only the redelivery of the messages it held. Rolling the whole batch back instead - as
     * this used to - also redelivers the records that were written and routed perfectly well, which is more
     * duplication than the failure warrants.
     *
     * @return true if the messages were routed, false if their content could not be written, in which case
     *         the FlowFile is discarded and the messages must be left unacknowledged
     */
    private boolean handleFailures(ProcessSession session,
                                   BlockingQueue<Message<GenericRecord>> parseFailures, byte[] demarcator) {

        if (CollectionUtils.isEmpty(parseFailures)) {
            return true;
        }

        FlowFile flowFile = session.create();
        final OutputStream rawOut = session.write(flowFile);

        try {
            writeParseFailures(rawOut, parseFailures, demarcator);
        } catch (final IOException e) {
            // The stream has to be closed before the FlowFile can be removed. Leaving it open - as the
            // error path used to - leaves a FlowFile dangling in the session that is neither routed nor
            // discarded, so the session cannot be committed.
            IOUtils.closeQuietly(rawOut);
            session.remove(flowFile);
            getLogger().error("Unable to write the messages that could not be parsed; they will be redelivered", e);
            return false;
        }

        IOUtils.closeQuietly(rawOut);
        session.transfer(flowFile, REL_PARSE_FAILURE);
        return true;
    }

    /**
     * Writes the unparseable messages into the parse-failure FlowFile, separated by the demarcator.
     * <p>
     * Extracted so the failure path above can be exercised: an IOException here must leave the session
     * clean rather than stranding a FlowFile with an open stream.
     *
     * @param out the FlowFile's output stream
     * @param parseFailures the messages that could not be parsed
     * @param demarcator bytes written between messages
     * @throws IOException if the content cannot be written
     */
    protected void writeParseFailures(final OutputStream out,
                                      final BlockingQueue<Message<GenericRecord>> parseFailures,
                                      final byte[] demarcator) throws IOException {

        final Iterator<Message<GenericRecord>> failureIterator = parseFailures.iterator();

        for (int idx = 0; failureIterator.hasNext(); idx++) {
            final Message<GenericRecord> msg = failureIterator.next();

            if (msg != null && msg.getData() != null) {
                if (idx > 0) {
                    out.write(demarcator);
                }

                out.write(msg.getData());
            }
        }
    }

    /**
     * Pull messages off of the CompletableFuture's held in the consumerService and process them in a batch.
     *
     * @param context       - The current ProcessContext
     * @param session       - The current ProcessSession.
     * @param consumer      - The Pulsar consumer.
     * @param readerFactory - The factory used to read the messages.
     * @param writerFactory - The factory used to write the messages.
     * @param demarcator    - The bytes used to demarcate the individual messages.
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
                    if (CollectionUtils.isNotEmpty(messages)) {
                        consumeMessages(context, session, consumer, messages, readerFactory, writerFactory, demarcator, true);
                    }
                }
            } while (done != null);

        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        } finally {
            drainAcknowledgments();
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
