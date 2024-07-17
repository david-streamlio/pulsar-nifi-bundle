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
package org.apache.nifi.processors.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.utils.PropertyMappingUtils;
import org.apache.nifi.processors.pulsar.utils.PublisherPool;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.cache.PulsarConsumerLRUCache;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public abstract class AbstractPulsarProducerProcessor<T> extends AbstractProcessor {

    public static final String MSG_COUNT = "msg.count";
    public static final String TOPIC_NAME = "topic.name";

    static final AllowableValue COMPRESSION_TYPE_NONE = new AllowableValue("NONE", "None", "No compression");
    static final AllowableValue COMPRESSION_TYPE_LZ4 = new AllowableValue("LZ4", "LZ4", "Compress with LZ4 algorithm.");
    static final AllowableValue COMPRESSION_TYPE_ZLIB = new AllowableValue("ZLIB", "ZLIB", "Compress with ZLib algorithm");

    static final AllowableValue MESSAGE_ROUTING_MODE_CUSTOM_PARTITION = new AllowableValue("CustomPartition", "Custom Partition", "Route messages to a custom partition");
    static final AllowableValue MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION = new AllowableValue("RoundRobinPartition", "Round Robin Partition", "Route messages to all "
                                                                                                                       + "partitions in a round robin manner");
    static final AllowableValue MESSAGE_ROUTING_MODE_SINGLE_PARTITION = new AllowableValue("SinglePartition", "Single Partition", "Route messages to a single partition");

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Pulsar.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Pulsar will be routed to this Relationship")
            .build();

    public static final PropertyDescriptor PULSAR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("PULSAR_CLIENT_SERVICE")
            .displayName("Pulsar Client Service")
            .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
            .required(true)
            .identifiesControllerService(PulsarClientService.class)
            .build();

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("TOPIC")
            .displayName("Topic Name")
            .description("The name of the Pulsar Topic.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ASYNC_ENABLED = new PropertyDescriptor.Builder()
            .name("ASYNC_ENABLED")
            .displayName("Async Enabled")
            .description("Control whether the messages will be sent asynchronously or not. Messages sent"
                    + " synchronously will be acknowledged immediately before processing the next message, while"
                    + " asynchronous messages will be acknowledged after the Pulsar broker responds. Running the"
                    + " processor with async enabled will result in increased the throughput at the risk of potential"
                    + " duplicate data being sent to the Pulsar broker.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor AUTO_UPDATE_PARTITIONS = new PropertyDescriptor.Builder()
            .name("AUTO_UPDATE_PARTITIONS")
            .displayName("Auto update partitions")
            .description("If enabled, the producer auto-subscribes for an increase in the number of partitions.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor AUTO_UPDATE_PARTITION_INTERVAL = new PropertyDescriptor.Builder()
            .name("AUTO_UPDATE_PARTITION_INTERVAL")
            .displayName("Auto Update Partition Interval")
            .description("Set the interval of updating partitions (default: 1 minute). This only works if " +
                    "autoUpdatePartitions is enabled.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 min")
            .required(false)
            .build();

    public static final PropertyDescriptor BATCHING_ENABLED = new PropertyDescriptor.Builder()
            .name("BATCHING_ENABLED")
            .displayName("Batching Enabled")
            .description("Control whether automatic batching of messages is enabled for the producer. "
                    + "default: false [No batching] When batching is enabled, multiple calls to "
                    + "Producer.sendAsync can result in a single batch to be sent to the broker, leading "
                    + "to better throughput, especially when publishing small messages. If compression is "
                    + "enabled, messages will be compressed at the batch level, leading to a much better "
                    + "compression ratio for similar headers or contents. When enabled default batch delay "
                    + "is set to 10 ms and default batch size is 1000 messages")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor BATCHING_MAX_BYTES = new PropertyDescriptor.Builder()
            .name("BATCHING_MAX_BYTES")
            .displayName("Batching Max Bytes")
            .description("Set the maximum number of bytes permitted in a batch. default: 128KB If set to a value greater" +
                    " than 0, messages will be queued until this threshold is reached or other batching conditions are met.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("128 KB")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor BATCHING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("BATCHING_MAX_MESSAGES")
            .displayName("Batching Max Messages")
            .description("Set the maximum number of messages permitted in a batch within the Pulsar client. "
                    + "default: 1000. If set to a value greater than 1, messages will be queued until this "
                    + "threshold is reached or the batch interval has elapsed, whichever happens first.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor BATCH_INTERVAL = new PropertyDescriptor.Builder()
            .name("BATCH_INTERVAL")
            .displayName("Batch Interval")
            .description("Set the time period within which the messages sent will be batched if batch messages are enabled."
                    + " If set to a non zero value, messages will be queued until this time interval has been reached OR"
                    + " until the Batching Max Messages threshold has been reached, whichever occurs first.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("10 ms")
            .build();

    public static final PropertyDescriptor CHUNKING_ENABLED = new PropertyDescriptor.Builder()
            .name("ENABLE_CHUNKING")
            .displayName("Enable chunking")
            .description("If message size is higher than allowed max publish-payload size by broker " +
                    "then enableChunking helps producer to split message into multiple chunks and " +
                    "publish them to broker separately and in order.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CHUNK_MAX_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("CHUNK_MAX_MESSAGE_SIZE")
            .displayName("Chunk Max Message Size")
            .description("Set the maximum size of message chunks (in bytes) permitted when message " +
                    "chunking is enabled. default: 500 MB.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("536870912")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor BLOCK_IF_QUEUE_FULL = new PropertyDescriptor.Builder()
            .name("BLOCK_IF_QUEUE_FULL")
            .displayName("Block if Message Queue Full")
            .description("Set whether the processor should block when the outgoing message queue is full. "
                    + "Default is false. If set to false, send operations will immediately fail with "
                    + "ProducerQueueIsFullError when there is no space left in pending queue.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("COMPRESSION_TYPE")
            .displayName("Compression Type")
            .description("Set the compression type for the producer.")
            .required(true)
            .allowableValues(COMPRESSION_TYPE_NONE, COMPRESSION_TYPE_LZ4, COMPRESSION_TYPE_ZLIB)
            .defaultValue(COMPRESSION_TYPE_NONE.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("MESSAGE_DEMARCATOR")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
                + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
                + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Pulsar message. "
                + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    public static final PropertyDescriptor MESSAGE_ROUTING_MODE = new PropertyDescriptor.Builder()
            .name("MESSAGE_ROUTING_MODE")
            .displayName("Message Routing Mode")
            .description("Set the message routing mode for the producer. This applies only if the destination topic is partitioned")
            .required(true)
            .allowableValues(MESSAGE_ROUTING_MODE_CUSTOM_PARTITION, MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION, MESSAGE_ROUTING_MODE_SINGLE_PARTITION)
            .defaultValue(MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION.getValue())
            .build();

    public static final PropertyDescriptor PENDING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("PENDING_MAX_MESSAGES")
            .displayName("Max Pending Messages")
            .description("Set the max size of the queue holding the messages pending to receive an "
                    + "acknowledgment from the broker.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor MAPPED_MESSAGE_PROPERTIES = new PropertyDescriptor.Builder()
            .name("MAPPED_MESSAGE_PROPERTIES")
            .displayName("Mapped Message Properties")
            .description("A comma-delimited list of message properties to set based on FlowFile attributes. "
                    + " Syntax for an individual property entry is <property name>[=<source attribute name>]."
                    + " If the optional source attribute name is omitted, it is assumed to be the same as the property.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor MESSAGE_KEY = new PropertyDescriptor.Builder()
            .name("MESSAGE_KEY")
            .displayName("Message Key")
            .description("he Key to use for the Message."
                    + "If not specified, the flow file attribute 'msg.key' is used as the message key, if it is present.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;

    static {
        List<PropertyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(PULSAR_CLIENT_SERVICE);
        descriptorList.add(TOPIC);
        descriptorList.add(ASYNC_ENABLED);
        descriptorList.add(AUTO_UPDATE_PARTITIONS);
        descriptorList.add(AUTO_UPDATE_PARTITION_INTERVAL);
        descriptorList.add(BATCHING_ENABLED);
        descriptorList.add(BATCHING_MAX_BYTES);
        descriptorList.add(BATCHING_MAX_MESSAGES);
        descriptorList.add(BATCH_INTERVAL);
        descriptorList.add(BLOCK_IF_QUEUE_FULL);
        descriptorList.add(CHUNKING_ENABLED);
        descriptorList.add(CHUNK_MAX_MESSAGE_SIZE);
        descriptorList.add(COMPRESSION_TYPE);
        descriptorList.add(MESSAGE_ROUTING_MODE);
        descriptorList.add(MESSAGE_DEMARCATOR);
        descriptorList.add(PENDING_MAX_MESSAGES);
        descriptorList.add(MAPPED_MESSAGE_PROPERTIES);
        descriptorList.add(MESSAGE_KEY);

        PROPERTIES = Collections.unmodifiableList(descriptorList);

        Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private PulsarClientService pulsarClientService;

    private PublisherPool publisherPool;

    @OnScheduled
    public void init(ProcessContext context) {
        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));
        setPublisherPool(createPublisherPool(context));
    }

    protected PublisherPool createPublisherPool(final ProcessContext context) {
        return new PublisherPool(getLogger(), getPulsarProducerConfiguration(context), this.getPulsarClientService().getPulsarClient());
    }

    protected Map<String, Object> getPulsarProducerConfiguration(ProcessContext ctx) {
        Map<String, Object> config = new HashMap<>();

        config.put("autoUpdatePartitions", ctx.getProperty(AUTO_UPDATE_PARTITIONS).asBoolean());
        config.put("autoUpdatePartitionsInterval", ctx.getProperty(AUTO_UPDATE_PARTITION_INTERVAL)
                .asTimePeriod(TimeUnit.SECONDS).intValue());
        config.put("blockIfQueueFull", ctx.getProperty(BLOCK_IF_QUEUE_FULL).asBoolean());
        config.put("compressionType", CompressionType.valueOf(ctx.getProperty(COMPRESSION_TYPE).getValue()));

        if (ctx.getProperty(BATCHING_ENABLED).asBoolean()) {
            config.put("batchingEnabled", Boolean.TRUE);
            config.put("batchingMaxBytes", ctx.getProperty(BATCHING_MAX_BYTES).asDataSize(DataUnit.B).intValue());
            config.put("batchingMaxMessages", ctx.getProperty(BATCHING_MAX_MESSAGES).evaluateAttributeExpressions().asInteger());
            config.put("batchingMaxPublishDelay", ctx.getProperty(BATCH_INTERVAL).evaluateAttributeExpressions()
                    .asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        } else {
            config.put("batchingEnabled", Boolean.FALSE);
            if (ctx.getProperty(CHUNKING_ENABLED).asBoolean()) {
                config.put("chunkingEnabled", Boolean.TRUE);
                config.put("chunkMaxMessageSize", ctx.getProperty(CHUNK_MAX_MESSAGE_SIZE).evaluateAttributeExpressions().asInteger());
            }
        }

        return config;
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

    protected synchronized PublisherPool getPublisherPool() {
        return this.publisherPool;
    }

    protected synchronized void setPublisherPool(PublisherPool pool) {
        this.publisherPool = pool;
    }

    protected byte[] getDemarcatorBytes(ProcessContext context, final FlowFile flowFile) {
        return context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8) : null;
    }

    protected String getMessageKey(ProcessContext context, final FlowFile flowFile) {
        String key = context.getProperty(MESSAGE_KEY).evaluateAttributeExpressions(flowFile).getValue();

        if (!StringUtils.isBlank(key)) {
            return key;
        }

        return null;
    }

    protected Map<String, String> getMappedMessageProperties(ProcessContext context, final FlowFile flowFile) {
        String mappings = context.getProperty(MAPPED_MESSAGE_PROPERTIES).getValue();
        return PropertyMappingUtils.getMappedValues(mappings, (a) -> flowFile.getAttribute(a));
    }

}
