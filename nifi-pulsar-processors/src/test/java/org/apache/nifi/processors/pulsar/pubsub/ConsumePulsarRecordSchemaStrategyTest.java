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

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * The Record Reader is required under the default strategy and optional under 'Topic Schema' (#185).
 * <p>
 * Making the descriptor itself optional would have silently accepted a configuration that has always been
 * invalid - a reader-driven consumer with no reader - so the requirement moved into validation instead.
 */
public class ConsumePulsarRecordSchemaStrategyTest extends AbstractPulsarProcessorTest<GenericRecord> {

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addPulsarClientService();

        final MockRecordWriter writer = new MockRecordWriter("id, reading");
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "test-topic");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "test-subscription");
    }

    /** The behaviour every existing flow has: no reader, no consumer. */
    @Test
    public void theRecordReaderIsRequiredUnderTheDefaultStrategy() {
        runner.assertNotValid();
    }

    /** Nothing changes for a flow that configures a reader and leaves the strategy alone. */
    @Test
    public void theDefaultStrategyIsValidWithAReader() throws InitializationException {
        addRecordReader();

        runner.assertValid();
    }

    /** The point of the strategy: the topic's schema replaces the reader entirely. */
    @Test
    public void theRecordReaderIsOptionalUnderTheTopicSchemaStrategy() {
        runner.setProperty(ConsumePulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");

        runner.assertValid();
    }

    /** A reader may still be configured alongside it, as the fallback for schema-less topics. */
    @Test
    public void aReaderMayStillBeConfiguredUnderTheTopicSchemaStrategy() throws InitializationException {
        runner.setProperty(ConsumePulsarRecord.MESSAGE_SCHEMA_STRATEGY, "Topic Schema");
        addRecordReader();

        runner.assertValid();
    }

    /**
     * The override must keep the superclass rules, not replace them (#194). Overriding customValidate with
     * a fresh list silently dropped both of the consumer's own rules for this processor, so a
     * ConsumePulsarRecord with no topic at all became a valid configuration and failed at runtime instead.
     */
    @Test
    public void theConsumersOwnValidationRulesStillApply() throws InitializationException {
        addRecordReader();
        runner.assertValid();

        runner.removeProperty(AbstractPulsarConsumerProcessor.TOPICS);
        runner.assertNotValid();
    }

    /** The other superclass rule, which the same bug disabled. */
    @Test
    public void theAcknowledgementTimeoutRuleStillApplies() throws InitializationException {
        addRecordReader();
        runner.assertValid();

        runner.setProperty(AbstractPulsarConsumerProcessor.ACK_TIMEOUT, "2 sec");
        runner.assertNotValid();
    }

    private void addRecordReader() throws InitializationException {
        final MockRecordParser reader = new MockRecordParser();
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
    }
}
