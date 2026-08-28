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
package org.apache.nifi.processors.pulsar.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises {@link ConsumePulsar} against a real broker. These cover the behaviour that the mocked unit
 * tests cannot reach: real message ids, real acknowledgement semantics and real subscription types.
 */
public class ConsumePulsarIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        // A brand new Pulsar subscription starts at the latest position, so anything published before the
        // processor first connects would never be delivered. The tests arrange messages up front, so they
        // read the topic from the beginning.
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
    }

    /** The basic contract: what is published is what comes out. */
    @Test
    public void consumesWhatWasPublished() throws Exception {
        final String topic = topic("roundtrip");
        subscribeTo(topic, "roundtrip-sub", 10);

        publish(topic, "one", "two", "three");

        final List<MockFlowFile> flowFiles = runUntilTransferred(1);
        flowFiles.get(0).assertContentEquals("one\ntwo\nthree");
        flowFiles.get(0).assertAttributeEquals(ConsumePulsar.MSG_COUNT, "3");
    }

    /**
     * The regression that motivated #142, verified end to end: with a real broker every message carries a
     * genuinely unique message id, which is exactly the condition the mocked tests could not reproduce.
     */
    @Test
    public void batchSizeIsHonouredWithRealMessageIds() throws Exception {
        final String topic = topic("batching");
        subscribeTo(topic, "batching-sub", 10);

        publish(topic, IntStream.rangeClosed(1, 10).mapToObj(n -> "m" + n).toArray(String[]::new));

        final List<MockFlowFile> flowFiles = runUntilTransferred(1);
        assertEquals("10 messages with a batch size of 10 must produce a single FlowFile", 1, flowFiles.size());
        flowFiles.get(0).assertAttributeEquals(ConsumePulsar.MSG_COUNT, "10");
        flowFiles.get(0).assertContentEquals(IntStream.rangeClosed(1, 10)
                .mapToObj(n -> "m" + n).collect(Collectors.joining("\n")));

        // real ids, so the first/last batch attributes are meaningful and distinct
        final String first = flowFiles.get(0).getAttribute("pulsar.message.id.first");
        final String last = flowFiles.get(0).getAttribute("pulsar.message.id.last");
        assertTrue("expected a real first message id, got " + first, first != null && !first.isEmpty());
        assertTrue("expected a real last message id, got " + last, last != null && !last.isEmpty());
        assertTrue("first and last ids should differ across a 10 message batch", !first.equals(last));
        flowFiles.get(0).assertAttributeNotExists("pulsar.message.id");
    }

    /** More messages than the batch size must split across FlowFiles rather than be dropped. */
    @Test
    public void moreMessagesThanBatchSizeSplitAcrossFlowFiles() throws Exception {
        final String topic = topic("split");
        subscribeTo(topic, "split-sub", 4);

        publish(topic, IntStream.rangeClosed(1, 10).mapToObj(n -> "m" + n).toArray(String[]::new));

        final List<MockFlowFile> flowFiles = runUntilTransferred(3);
        assertEquals("4", flowFiles.get(0).getAttribute(ConsumePulsar.MSG_COUNT));
        assertEquals("4", flowFiles.get(1).getAttribute(ConsumePulsar.MSG_COUNT));
        assertEquals("2", flowFiles.get(2).getAttribute(ConsumePulsar.MSG_COUNT));
    }

    /**
     * Acknowledgements really reach the broker: a second subscriber on the same subscription must not be
     * redelivered what the first one already consumed and acked.
     */
    @Test
    public void acknowledgedMessagesAreNotRedelivered() throws Exception {
        final String topic = topic("acks");
        subscribeTo(topic, "acks-sub", 10);

        publish(topic, "a", "b", "c");
        runUntilTransferred(1);

        // a fresh runner on the SAME subscription should find nothing left to consume
        runner.clearTransferState();
        runner.run(3, true);
        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
    }

    // ------------------------------------------------------------------ helpers

    /** Unique per test so the tests sharing one broker cannot interfere with each other. */
    private static String topic(final String name) {
        return "persistent://public/default/" + name + "-" + System.nanoTime();
    }

    private void subscribeTo(final String topic, final String subscription, final int batchSize) {
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, subscription);
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, String.valueOf(batchSize));
    }

    /** Triggers until the expected number of FlowFiles has been produced, so timing cannot flake the test. */
    private List<MockFlowFile> runUntilTransferred(final int expected) throws Exception {
        await(expected + " FlowFile(s) transferred to success", () -> {
            runner.run(1, false);
            return runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).size() >= expected;
        });
        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, expected);
        return runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
    }
}
