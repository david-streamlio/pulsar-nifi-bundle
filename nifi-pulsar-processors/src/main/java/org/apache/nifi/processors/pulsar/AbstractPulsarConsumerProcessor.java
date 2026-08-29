/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.utils.PropertyMappingUtils;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.cache.PulsarConsumerLRUCache;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;

public abstract class AbstractPulsarConsumerProcessor<T> extends AbstractProcessor {
    protected static final String PULSAR_MESSAGE_KEY = "__KEY__";

    protected static final AllowableValue EXCLUSIVE = new AllowableValue("Exclusive", "Exclusive", "There can be only 1 consumer on the same topic with the same subscription name");
    protected static final AllowableValue KEY_SHARED = new AllowableValue("Key_Shared", "Key_Shared", "Multiple consumers will be able to use the same subscription name and messages "
    		+ "but only 1 consumer will receive the messages for a given message key.");
    protected static final AllowableValue SHARED = new AllowableValue("Shared", "Shared", "Multiple consumer will be able to use the same subscription name and the messages");
    protected static final AllowableValue FAILOVER = new AllowableValue("Failover", "Failover", "Multiple consumer will be able to use the same subscription name but only 1 consumer "
            + "will receive the messages. If that consumer disconnects, one of the other connected consumers will start receiving messages.");

    static final AllowableValue CONSUME = new AllowableValue(ConsumerCryptoFailureAction.CONSUME.name(), "Consume",
            "Mark the message as consumed despite being unable to decrypt the contents");
    static final AllowableValue DISCARD = new AllowableValue(ConsumerCryptoFailureAction.DISCARD.name(), "Discard",
            "Discard the message and don't perform any addtional processing on the message");
    static final AllowableValue FAIL = new AllowableValue(ConsumerCryptoFailureAction.FAIL.name(), "Fail",
            "Report a failure condition, and then route the message contents to the FAILED relationship.");

    static final AllowableValue OFFSET_EARLIEST = new AllowableValue("Earliest", "Earliest",
            "The earliest position which means the start consuming position will be the first message.");
    static final AllowableValue OFFSET_LATEST = new AllowableValue("Latest", "Latest",
            "The latest position which means the start consuming position will be the last message.");

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was consumed from Pulsar.")
            .build();

    public static final PropertyDescriptor PULSAR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("PULSAR_CLIENT_SERVICE")
            .displayName("Pulsar Client Service")
            .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
            .required(true)
            .identifiesControllerService(PulsarClientService.class)
            .build();

    public static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("TOPICS")
            .displayName("Topic Names")
            .description("Specify the topics this consumer will subscribe on. "
                    + "You can specify multiple topics in a comma-separated list."
                    + "E.g topicA, topicB, topicC ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOPICS_PATTERN = new PropertyDescriptor.Builder()
            .name("TOPICS_PATTERN")
            .displayName("Topics Pattern")
            .description("Alternatively, you can specify a pattern for topics that this consumer "
                    + "will subscribe on. It accepts a regular expression and will be compiled into "
                    + "a pattern internally. E.g. \"persistent://my-tenant/ns-abc/pattern-topic-.*\" "
                    + "would subscribe to any topic whose name started with 'pattern-topic-' that was in "
                    + "the 'ns-abc' namespace, and belonged to the 'my-tenant' tentant.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_NAME = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_NAME")
            .displayName("Subscription Name")
            .description("Specify the subscription name for this consumer.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_INITIAL_POSITION = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_INITIAL_POSITION")
            .displayName("Subscription Initial Position")
            .description("Specify subscription initial position. By default the subscription "
                    + "will be created at the end of the topic.")
            .required(false)
            .allowableValues(OFFSET_EARLIEST, OFFSET_LATEST)
            .defaultValue(OFFSET_LATEST.getValue())
            .build();

    public static final PropertyDescriptor ASYNC_ENABLED = new PropertyDescriptor.Builder()
            .name("ASYNC_ENABLED")
            .displayName("Async Enabled")
            .description("Control whether the messages will be consumed asynchronously or not. Messages consumed"
                    + " synchronously will be acknowledged immediately before processing the next message, while"
                    + " asynchronous messages will be acknowledged after the Pulsar broker responds. \n"
                    + "Enabling asynchronous message consumption introduces the possibility of duplicate data "
                    + "consumption in the case where the Processor is stopped before it has time to send an "
                    + "acknowledgement back to the Broker. In this scenario, the Broker would assume that the "
                    + "un-acknowledged message was not successfully processed and re-send it when the Processor restarted.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor AUTO_UPDATE_PARTITIONS = new PropertyDescriptor.Builder()
            .name("AUTO_UPDATE_PARTITIONS")
            .displayName("Auto update partitions")
            .description("If enabled, the consumer auto-subscribes for an increase in the number of partitions.")
            .required(true)
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

    public static final PropertyDescriptor MAX_ASYNC_REQUESTS = new PropertyDescriptor.Builder()
            .name("MAX_ASYNC_REQUESTS")
            .displayName("Maximum Async Requests")
            .description("The maximum number of outstanding asynchronous consumer requests for this processor. "
                    + "Each asynchronous call requires memory, so avoid setting this value to high.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();

    public static final PropertyDescriptor CONSUMER_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("CONSUMER_CACHE_SIZE")
            .displayName("Consumer Cache Size")
            .description("The maximum number of Pulsar consumers this processor keeps open at once. When the "
                    + "cache is full the least recently used consumer is closed to make room, which unsubscribes "
                    + "it until it is needed again. Set this at or above the number of topics a Topics Pattern "
                    + "subscription is expected to match, otherwise consumers will be closed and reopened as the "
                    + "processor cycles through them.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("20")
            .build();

    public static final PropertyDescriptor ACK_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ACK_TIMEOUT")
            .displayName("Acknowledgment Timeout")
            .description("Set the timeout for unacked messages. Messages that are not acknowledged within the "
                    + "configured timeout will be replayed. This value needs to be greater than 10 seconds.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .required(false)
            .build();

    public static final PropertyDescriptor EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE = new PropertyDescriptor.Builder()
            .name("EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE")
            .displayName("Expire Time of Incomplete Chunked Message")
            .description("If producer fails to publish all the chunks of a message then consumer can expire incomplete" +
                    " chunks if consumer won't be able to receive all chunks in expire times (default 1 minute).")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("60 sec")
            .required(false)
            .build();

    public static final PropertyDescriptor AUTO_ACK_OLDEST_CHUNKED_ON_QUEUE_FULL = new PropertyDescriptor.Builder()
            .name("AUTO_ACK_OLDEST_CHUNKED_ON_QUEUE_FULL")
            .displayName("Auto Ack Oldest Chunked Message on Queue Full")
            .description("Buffering large number of outstanding uncompleted chunked messages can create memory pressure" +
                    " and it can be guarded by providing this @maxPendingChunkedMessage threshold. Once, consumer reaches" +
                    " this threshold, it drops the outstanding unchunked-messages by silently acknowledging if this property" +
                    " is true else it marks them for redelivery.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_PENDING_CHUNKED_MESSAGE = new PropertyDescriptor.Builder()
            .name("MAX_PENDING_CHUNKED_MESSAGE")
            .displayName("Maximum Pending Chunked Messages")
            .description("Buffering large number of outstanding uncompleted chunked messages can create memory pressure " +
                    "and it can be guarded by providing this @maxPendingChunkedMessage threshold. Once, consumer reaches" +
                    " this threshold, it drops the outstanding unchunked-messages by silently acking or asking broker to" +
                    " redeliver later by marking it unacked. This behavior can be controlled by the " +
                    "AUTO_ACK_OLDEST_CHUNKED_ON_QUEUE_FULL property.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor CONSUMER_NAME = new PropertyDescriptor.Builder()
            .name("CONSUMER_NAME")
            .displayName("Consumer Name")
            .description("Set the name of the consumer to uniquely identify this client on the Broker")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRIORITY_LEVEL = new PropertyDescriptor.Builder()
            .name("PRIORITY_LEVEL")
            .displayName("Consumer Priority Level")
            .description("Sets priority level for the shared subscription consumers to which broker "
                    + "gives more priority while dispatching messages. Here, broker follows descending "
                    + "priorities. (eg: 0=max-priority, 1, 2,..) ")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor RECEIVER_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("RECEIVER_QUEUE_SIZE")
            .displayName("Consumer Receiver Queue Size")
            .description("The consumer receive queue controls how many messages can be accumulated "
                    + "by the Consumer before the application calls Consumer.receive(). Using a higher "
                    + "value could potentially increase the consumer throughput at the expense of bigger "
                    + "memory utilization. \n"
                    + "Setting the consumer queue size as zero, \n"
                    + "\t - Decreases the throughput of the consumer, by disabling pre-fetching of messages. \n"
                    + "\t - Doesn't support Batch-Message: if consumer receives any batch-message then it closes consumer "
                    + "connection with broker and consumer will not be able receive any further message unless batch-message "
                    + "in pipeline is removed")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_TYPE = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_TYPE")
            .displayName("Subscription Type")
            .description("Select the subscription type to be used when subscribing to the topic.")
            .required(true)
            .allowableValues(EXCLUSIVE, SHARED, KEY_SHARED, FAILOVER)
            .defaultValue(SHARED.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("MESSAGE_DEMARCATOR")
            .displayName("Message Demarcator")
            .required(true)
            .addValidator(Validator.VALID)
            .defaultValue("\n")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages consumed from Pulsar within "
                + "a single FlowFile. If not specified, the content of the FlowFile will consist of all of the messages consumed from Pulsar "
                + "concatenated together. If specified, the contents of the individual Pulsar messages will be separated by this delimiter. "
                + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    public static final PropertyDescriptor CONSUMER_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("CONSUMER_BATCH_SIZE")
            .displayName("Consumer Message Batch Size")
            .description("Set the maximum number of messages consumed at a time, and published to a single FlowFile. "
                    + "default: 1000. If set to a value greater than 1, messages within the FlowFile will be seperated "
                    + "by the Message Demarcator. Consecutive messages are written to the same FlowFile as long as their "
                    + "Mapped FlowFile Attributes are identical; a change in those attributes starts a new FlowFile before "
                    + "the batch size is reached. ConsumePulsarRecord also starts a new FlowFile when the record schema "
                    + "changes: with a Record Reader that infers the schema from each message, a batch of messages of "
                    + "different shapes is split into one FlowFile per shape change rather than losing the fields that "
                    + "the first message of the batch does not have.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAPPED_FLOWFILE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("MAPPED_FLOWFILE_ATTRIBUTES")
            .displayName("Mapped FlowFile Attributes")
            .description("A comma-delimited list of FlowFile attributes to set based on message metadata (currently key and properties)."
                    + " Syntax for an individual mapping is <attribute name>[=<source property name or key>]."
                    + " To map the message key to an attribute, use the reserved name __KEY__ (ex. my-attribute=__KEY__ )."
                    + " If the optional source name is omitted, it is assumed to be the same as the attribute."
                    + " These attributes also determine which messages may share a FlowFile: consecutive messages with"
                    + " identical mapped values are batched together (up to the Consumer Message Batch Size), while a change"
                    + " in any mapped value starts a new FlowFile. Message metadata that is not mapped here (message id,"
                    + " message properties) is added to the FlowFile as 'pulsar.message.id*' / 'pulsar.property.*' attributes"
                    + " but never splits a batch.")
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor REPLICATE_SUBSCRIPTION_STATE = new PropertyDescriptor.Builder()
            .name("REPLICATE_SUBSCRIPTION_STATE")
            .displayName("Replicate Subscription State")
            .description("Control whether to replicate subscription state across multiple geographical regions "
                    + "in case the topic is geo-replicated. In case of failover, the consumer can restart consuming "
                    + "from the failure point in a different cluster.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;

    static {
        List<PropertyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(PULSAR_CLIENT_SERVICE);
        descriptorList.add(TOPICS);
        descriptorList.add(TOPICS_PATTERN);
        descriptorList.add(SUBSCRIPTION_NAME);
        descriptorList.add(SUBSCRIPTION_INITIAL_POSITION);
        descriptorList.add(CONSUMER_CACHE_SIZE);
        descriptorList.add(CONSUMER_NAME);
        descriptorList.add(ASYNC_ENABLED);
        descriptorList.add(MAX_ASYNC_REQUESTS);
        descriptorList.add(ACK_TIMEOUT);
        descriptorList.add(MAX_PENDING_CHUNKED_MESSAGE);
        descriptorList.add(AUTO_ACK_OLDEST_CHUNKED_ON_QUEUE_FULL);
        descriptorList.add(EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE);
        descriptorList.add(MAX_PENDING_CHUNKED_MESSAGE);
        descriptorList.add(AUTO_UPDATE_PARTITIONS);
        descriptorList.add(AUTO_UPDATE_PARTITION_INTERVAL);
        descriptorList.add(PRIORITY_LEVEL);
        descriptorList.add(RECEIVER_QUEUE_SIZE);
        descriptorList.add(SUBSCRIPTION_TYPE);
        descriptorList.add(CONSUMER_BATCH_SIZE);
        descriptorList.add(MESSAGE_DEMARCATOR);
        descriptorList.add(MAPPED_FLOWFILE_ATTRIBUTES);
        descriptorList.add(REPLICATE_SUBSCRIPTION_STATE);

        PROPERTIES = Collections.unmodifiableList(descriptorList);

        Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(relationshipSet);

    }

    private PulsarClientService pulsarClientService;
    private PulsarConsumerLRUCache<String, Consumer<GenericRecord>> consumers;
    private ExecutorService consumerPool;
    private ExecutorCompletionService<List<Message<GenericRecord>>> consumerService;
    private ExecutorService ackPool;
    private ExecutorCompletionService<Object> ackService;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        boolean topicsSet = validationContext.getProperty(TOPICS).isSet();
        boolean topicPatternSet = validationContext.getProperty(TOPICS_PATTERN).isSet();

        if (!topicsSet && !topicPatternSet) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "At least one of the 'Topics' or 'Topic Pattern' properties must be specified.").build());
        } else if (topicsSet && topicPatternSet) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "Only one of the two properties ('Topics' and 'Topic Pattern') can be specified.").build());
        }

        if (validationContext.getProperty(ACK_TIMEOUT).asTimePeriod(TimeUnit.SECONDS) < 10) {
           results.add(new ValidationResult.Builder().valid(false).explanation(
               "Acknowledgment Timeout needs to be greater than 10 seconds.").build());
        }

        return results;
    }

    @OnScheduled
    public void init(ProcessContext context) {
        // Record the size only. Replacing the cache here would abandon the consumers the previous one
        // holds without closing them, and the broker then refuses the replacement consumer on an
        // Exclusive subscription with "Exclusive consumer is already connected". The cache is built
        // lazily below and disposed in cleanUp().
        this.consumerCacheSize = context.getProperty(CONSUMER_CACHE_SIZE).asInteger();

        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            setConsumerPool(Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger()));
            setConsumerService(new ExecutorCompletionService<>(getConsumerPool()));
            setAckPool(Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger() + 1));
            setAckService(new ExecutorCompletionService<>(getAckPool()));
        }

        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
        /*
         * If we are running in asynchronous mode, then we need to stop all the consumer threads that
         * are running in the ConsumerPool. After, we have stopped them, we need to wait a bit
         * to ensure that all the messages are properly acked, in order to prevent re-processing the
         * same messages in the event of a shutdown and restart of the processor since the un-acked
         * messages would be replayed on startup.
         */
        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            try {
                getConsumerPool().shutdown();
                getAckPool().shutdown();

                // Allow some time for the acks to be sent back to the Broker.
                getConsumerPool().awaitTermination(10, TimeUnit.SECONDS);
                getAckPool().awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                getLogger().error("Unable to stop all the Pulsar Consumers", e);
            }
        }
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
        shutDown(context);
        // clear() closes each cached consumer
        getConsumers().clear();
        // drop the cache itself so a restart rebuilds it at the currently configured size
        setConsumers(null);
    }

    /**
     * Method returns a string that uniquely identifies a consumer by concatenating
     * the topic name and subscription properties together.
     * 
     * @param context - The Processor context
     * @param flowFile - The current NiFi flow file
     * @return The consumer id.
     */
    protected String getConsumerId(final ProcessContext context, FlowFile flowFile) {
        if (context == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer();

        if (context.getProperty(TOPICS).isSet()) {
           sb.append(context.getProperty(TOPICS).evaluateAttributeExpressions(flowFile).getValue());
        } else {
           sb.append(context.getProperty(TOPICS_PATTERN).getValue());
        }

        sb.append("-").append(context.getProperty(SUBSCRIPTION_NAME).getValue());

        if (context.getProperty(CONSUMER_NAME).isSet()) {
            sb.append("-").append(context.getProperty(CONSUMER_NAME).getValue());
        }
        return sb.toString();
    }

    protected void consumeAsync(final Consumer<GenericRecord> consumer, 
    	ProcessContext context, ProcessSession session) throws PulsarClientException {
        try {
            final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                    .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

            getConsumerService().submit(() -> {
                List<Message<GenericRecord>> messages = new LinkedList<Message<GenericRecord>>();
                Message<GenericRecord> msg = null;
                AtomicInteger msgCount = new AtomicInteger(0);

                while (msgCount.get() < maxMessages && (msg = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                    messages.add(msg);
                    msgCount.incrementAndGet();
                }

                return messages;
            });
        } catch (final RejectedExecutionException ex) {
            getLogger().error("Unable to consume any more Pulsar messages", ex);
            context.yield();
        }
    }

    protected synchronized Consumer<GenericRecord> getConsumer(ProcessContext context, String topic) throws PulsarClientException {

        /* Avoid creating producers for non-existent topics */
        if (StringUtils.isBlank(topic)) {
            return null;
        }

        Consumer<GenericRecord> consumer = getConsumers().get(topic);

	// The Pulsar client will automatically reconnect consumers when disconnected
        if (consumer != null) {
            return consumer;
        }

        consumer = getConsumerBuilder(context).subscribe();
        getConsumers().put(topic, consumer);

        return consumer;
    }

	protected synchronized ConsumerBuilder<GenericRecord> getConsumerBuilder(ProcessContext context) throws PulsarClientException {
    	
		ConsumerBuilder<GenericRecord> builder = 
			getPulsarClientService().getPulsarClient().newConsumer(Schema.AUTO_CONSUME());

        if (context.getProperty(TOPICS).isSet()) {
        	String[] topics = Arrays.stream(context.getProperty(TOPICS).evaluateAttributeExpressions().getValue().split("[, ]"))
                    .map(String::trim).toArray(String[]::new);
        	
            builder = builder.topic(topics);
        } else if (context.getProperty(TOPICS_PATTERN).isSet()) {
        	String topicsPattern = context.getProperty(TOPICS_PATTERN).getValue();
            builder = builder.topicsPattern(topicsPattern);
        }

        if (context.getProperty(CONSUMER_NAME).isSet()) {
            builder = builder.consumerName(context.getProperty(CONSUMER_NAME).getValue());
        }

        return builder.subscriptionName(context.getProperty(SUBSCRIPTION_NAME).getValue())
                .subscriptionInitialPosition(SubscriptionInitialPosition.valueOf(context.getProperty(SUBSCRIPTION_INITIAL_POSITION).getValue()))
                .autoUpdatePartitions(context.getProperty(AUTO_UPDATE_PARTITIONS).asBoolean())
                .autoUpdatePartitionsInterval(context.getProperty(AUTO_UPDATE_PARTITION_INTERVAL)
                        .asTimePeriod(TimeUnit.SECONDS).intValue(), TimeUnit.SECONDS)
                .ackTimeout(context.getProperty(ACK_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
                .autoAckOldestChunkedMessageOnQueueFull(context.getProperty(AUTO_ACK_OLDEST_CHUNKED_ON_QUEUE_FULL).asBoolean())
                .expireTimeOfIncompleteChunkedMessage(context.getProperty(EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE)
                        .asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS)
                .maxPendingChunkedMessage(context.getProperty(MAX_PENDING_CHUNKED_MESSAGE).asInteger())
                .priorityLevel(context.getProperty(PRIORITY_LEVEL).asInteger())
                .receiverQueueSize(context.getProperty(RECEIVER_QUEUE_SIZE).asInteger())
                .subscriptionType(SubscriptionType.valueOf(context.getProperty(SUBSCRIPTION_TYPE).getValue()))
                .replicateSubscriptionState(context.getProperty(REPLICATE_SUBSCRIPTION_STATE).asBoolean());
    }

	protected synchronized ExecutorService getConsumerPool() {
        return consumerPool;
    }

    protected synchronized void setConsumerPool(ExecutorService pool) {
        this.consumerPool = pool;
    }

    protected synchronized ExecutorCompletionService<List<Message<GenericRecord>>> getConsumerService() {
        return consumerService;
    }

    protected synchronized void setConsumerService(ExecutorCompletionService<List<Message<GenericRecord>>> service) {
        this.consumerService = service;
    }

    protected synchronized ExecutorService getAckPool() {
       return ackPool;
    }

    protected synchronized void setAckPool(ExecutorService pool) {
       this.ackPool = pool;
    }

    /**
     * Consumes every acknowledgement task that has already completed.
     * <p>
     * Acknowledgements in asynchronous mode are submitted to an {@link ExecutorCompletionService}, which
     * retains the {@link Future} of every completed task in an unbounded internal queue until it is taken.
     * Nothing used to take them, so the queue grew by one Future per acknowledgement - one per message on
     * Shared/Key_Shared subscriptions, one per batch otherwise - for as long as the processor ran, and the
     * memory was only released when the processor was stopped and the pools rebuilt.
     * <p>
     * Draining after each trigger bounds the queue by the acknowledgements still in flight, and gives us the
     * result of each one: a failed acknowledgement used to be captured in a Future that nobody ever read, so
     * a broker rejecting acks failed completely silently.
     */
    protected void drainAcknowledgments() {
        final ExecutorCompletionService<Object> service = getAckService();

        if (service == null) {
            return;
        }

        Future<Object> ack = service.poll();

        while (ack != null) {
            try {
                ack.get();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (final ExecutionException e) {
                getLogger().warn("Failed to acknowledge a Pulsar message", e.getCause());
            } catch (final CancellationException e) {
                getLogger().warn("Acknowledgement of a Pulsar message was cancelled", e);
            }

            ack = service.poll();
        }
    }

    /**
     * Commits the session and, once the commit has succeeded, acknowledges {@code messages} to the broker.
     * <p>
     * An acknowledged message is gone from the subscription: the broker never redelivers it. So a message
     * may only be acknowledged once the FlowFile that carries it has been committed. Acknowledging earlier
     * - as each message was claimed, or on a path that then rolled the session back - told the broker the
     * message was handled while its content could still be discarded, and a write error (a full content
     * repository, a permissions problem, a disk fault) lost the message for good: it was neither in NiFi
     * nor recoverable from Pulsar. The acknowledgement now runs in the commit callback, so a session that
     * is never committed acknowledges nothing and the broker redelivers its messages instead.
     * <p>
     * Shared and Key_Shared subscriptions do not permit cumulative acknowledgements, so every message is
     * acknowledged individually; the other subscription types acknowledge cumulatively up to the last
     * message. In asynchronous mode the acknowledgements are submitted to the acknowledgement service and
     * collected by {@link #drainAcknowledgments()}, as before. {@code messages} is emptied so the caller
     * can keep collecting the messages of the next commit in the same list.
     *
     * @param session  the session holding the FlowFiles the messages were written to
     * @param consumer the consumer the messages were received from
     * @param messages the messages carried by the FlowFiles in the session; cleared on return
     * @param shared   whether the subscription is Shared or Key_Shared
     * @param async    whether to acknowledge through the asynchronous acknowledgement service
     */
    protected void commitAndAcknowledge(final ProcessSession session, final Consumer<GenericRecord> consumer,
                                        final List<Message<GenericRecord>> messages, final boolean shared, final boolean async) {
        if (messages.isEmpty()) {
            return;
        }

        final List<Message<GenericRecord>> committed = new ArrayList<>(messages);
        messages.clear();

        session.commitAsync(() -> acknowledge(consumer, committed, shared, async));
    }

    private void acknowledge(final Consumer<GenericRecord> consumer, final List<Message<GenericRecord>> messages,
                             final boolean shared, final boolean async) {
        final ExecutorCompletionService<Object> service = async ? getAckService() : null;

        try {
            if (shared) {
                for (final Message<GenericRecord> message : messages) {
                    if (service != null) {
                        service.submit(() -> consumer.acknowledgeAsync(message).get());
                    } else {
                        consumer.acknowledge(message);
                    }
                }
            } else {
                final Message<GenericRecord> last = messages.get(messages.size() - 1);

                if (service != null) {
                    service.submit(() -> consumer.acknowledgeCumulativeAsync(last).get());
                } else {
                    consumer.acknowledgeCumulative(last);
                }
            }
        } catch (final PulsarClientException e) {
            // The FlowFiles are already committed, so nothing is lost: the broker redelivers whatever was
            // not acknowledged and the flow sees those messages again.
            getLogger().error("Unable to acknowledge {} message(s) whose FlowFiles were committed; the broker will redeliver them",
                    messages.size(), e);
        }
    }

    protected synchronized ExecutorCompletionService<Object> getAckService() {
       return ackService;
    }

    protected synchronized void setAckService(ExecutorCompletionService<Object> ackService) {
       this.ackService = ackService;
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

    /** Used when the processor has not been scheduled, i.e. outside a running flow. */
    static final int DEFAULT_CONSUMER_CACHE_SIZE = 20;

    private volatile int consumerCacheSize = DEFAULT_CONSUMER_CACHE_SIZE;

    protected synchronized PulsarConsumerLRUCache<String, Consumer<GenericRecord>> getConsumers() {
        if (consumers == null) {
           consumers = new PulsarConsumerLRUCache<String, Consumer<GenericRecord>>(consumerCacheSize);
        }
        return consumers;
    }

    protected void setConsumers(PulsarConsumerLRUCache<String, Consumer<GenericRecord>> consumers) {
        this.consumers = consumers;
    }

    /**
     * Returns the attributes that decide whether a message may share a FlowFile with the previous one:
     * the user-configured "Mapped FlowFile Attributes" only. Consecutive messages whose mapped values are
     * identical are written to the same FlowFile (up to the Consumer Message Batch Size); a change in any
     * mapped value closes the current FlowFile and starts a new one.
     * <p>
     * Per-message metadata such as the message id or the message properties is deliberately NOT part of
     * this map: it changes with every message, so including it here would close the FlowFile after each
     * message and defeat batching. That metadata is added to the FlowFile through
     * {@link org.apache.nifi.processors.pulsar.utils.MessageBatchAttributes}, with semantics that stay
     * coherent when a FlowFile contains several messages.
     *
     * @param context - The Processor context
     * @param msg - The message being consumed
     * @return the mapped attribute values of the message (never null, possibly empty)
     */
    protected Map<String, String> getMappedFlowFileAttributes(ProcessContext context, final Message<GenericRecord> msg) {
        String mappings = context.getProperty(MAPPED_FLOWFILE_ATTRIBUTES).getValue();

        return PropertyMappingUtils.getMappedValues(mappings,
        		(p) -> PULSAR_MESSAGE_KEY.equals(p) ? msg.getKey() : msg.getProperty(p));
    }
    
    protected boolean isSharedSubscription(ProcessContext context) {
    	final String subscriptionType = context.getProperty(SUBSCRIPTION_TYPE).getValue();
    	
    	return subscriptionType.equalsIgnoreCase(SHARED.getValue()) || subscriptionType.equalsIgnoreCase(KEY_SHARED.getValue());
    }
}
