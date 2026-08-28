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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Messages with an empty payload must not produce an empty FlowFile.
 * <p>
 * The synchronous path has always guarded its transfers with {@code if (msgCount.get() < 1)} and removed
 * the FlowFile instead. The asynchronous path transferred unconditionally, so a poll in which every
 * message had an empty payload emitted a FlowFile with no content and {@code message.count} of 0. The two
 * paths disagreed; this pins them to the same behaviour.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarEmptyMessageTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/empty-payloads";

    @Parameters(name = "async={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    private final boolean async;

    public ConsumePulsarEmptyMessageTest(final boolean async) {
        this.async = async;
    }

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
    }

    /** A batch of nothing but empty payloads should emit nothing at all. */
    @Test
    public void allEmptyPayloadsProduceNoFlowFile() {
        mockClientService.setMockMessageQueue(messages(new byte[0], new byte[0], new byte[0]));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
    }

    /** A null payload is the same case and must be treated the same way. */
    @Test
    public void nullPayloadsProduceNoFlowFile() {
        mockClientService.setMockMessageQueue(messages(null, null));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
    }

    /** Mixed content still comes through, carrying only the messages that had a payload. */
    @Test
    public void emptyPayloadsAreSkippedButRealOnesArrive() {
        mockClientService.setMockMessageQueue(messages(
                "one".getBytes(UTF_8), new byte[0], "two".getBytes(UTF_8)));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 1);
        final org.apache.nifi.util.MockFlowFile flowFile =
                runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("one\ntwo");
        assertEquals("2", flowFile.getAttribute(ConsumePulsar.MSG_COUNT));
    }

    private static List<Message<GenericRecord>> messages(final byte[]... payloads) {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();
        int n = 0;
        for (final byte[] payload : payloads) {
            msgs.add(new MockPulsarMessage<GenericRecord>(TOPIC, payload, "1234:" + (++n) + ":0", null, null));
        }
        return msgs;
    }
}
