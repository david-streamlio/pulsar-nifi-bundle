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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Before;
import org.junit.Test;

/**
 * An exclusive <i>Producer Access Mode</i> on a {@code PublishPulsarRecord} that runs more than one concurrent task,
 * against a real broker. Nothing else writes the topic, so the only producer the processor can ever collide with
 * is its own: before #219 the pool opened one producer per concurrent lease, and the broker refused the second
 * ({@code Exclusive}), let it fence the first ({@code ExclusiveWithFencing}) or held it waiting for a producer that
 * never closed ({@code WaitForExclusive}).
 * <p>
 * The invariant is the same for all three: every FlowFile succeeds, none fails, every record reaches the topic,
 * and the run finishes.
 */
public class PublishPulsarExclusiveAccessIT extends AbstractPulsarIT {

    private static final int FLOWFILES = 10;

    /**
     * One record per FlowFile, padded to 600 KB so the 1 MB size-based filter hands each trigger exactly one
     * FlowFile: that is what makes the two tasks publish at the same time instead of one task draining the
     * queue in a single trigger.
     */
    private static final String PADDING = "x".repeat(600_000);

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);
        addRealPulsarClientService(runner, "pulsar-client");

        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.STRING);
        reader.addSchemaField("reading", RecordFieldType.INT);
        runner.addControllerService("record-reader", reader);
        runner.enableControllerService(reader);

        final MockRecordWriter writer = new MockRecordWriter("id, reading");
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(PublishPulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
        runner.setThreadCount(2);
    }

    @Test(timeout = 120_000)
    public void twoTasksShareTheExclusiveProducer() throws Exception {
        twoTasksPublishEverything(ProducerAccessMode.Exclusive);
    }

    @Test(timeout = 120_000)
    public void twoTasksDoNotFenceEachOther() throws Exception {
        twoTasksPublishEverything(ProducerAccessMode.ExclusiveWithFencing);
    }

    /** The timeout is the assertion here: before the fix the second task never returned from producer creation. */
    @Test(timeout = 120_000)
    public void twoTasksDoNotWaitForEachOtherForever() throws Exception {
        twoTasksPublishEverything(ProducerAccessMode.WaitForExclusive);
    }

    private void twoTasksPublishEverything(final ProducerAccessMode mode) throws Exception {
        final String topic = "persistent://public/default/exclusive-" + mode.name().toLowerCase() + "-" + System.nanoTime();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);
        runner.setProperty(AbstractPulsarProducerProcessor.ACCESS_MODE, mode.name());

        // Everything is queued up front, so that while more than one FlowFile is waiting both tasks find work
        // in the same trigger round and publish concurrently. (MockProcessSession briefly removes a FlowFile
        // the size filter rejected while it is being put back, so a task can also find the queue empty for an
        // instant and yield; the loop simply runs another round until the queue is drained.)
        final Set<String> expectedIds = new HashSet<>();
        for (int n = 0; n < FLOWFILES; n++) {
            final String id = "sensor-" + n;
            expectedIds.add(id);
            runner.enqueue((id + PADDING + "," + n).getBytes(UTF_8));
        }

        boolean initialize = true;
        int rounds = 0;
        while (runner.getQueueSize().getObjectCount() > 0 && rounds++ < FLOWFILES * 2) {
            // 60 s run wait: the default 5 s is not a bound a task that queues behind its sibling's lease and then
            // publishes a 600 KB message itself is guaranteed to meet.
            runner.run(2, false, initialize, 60_000);
            initialize = false;
        }

        runner.assertTransferCount(PublishPulsarRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PublishPulsarRecord.REL_SUCCESS, FLOWFILES);
        assertEquals("every FlowFile must have been processed", 0, runner.getQueueSize().getObjectCount());

        final Set<String> receivedIds = new HashSet<>();
        try (Consumer<byte[]> consumer = getClient().newConsumer(Schema.BYTES)
                .topic(topic).subscriptionName("exclusive-check")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<byte[]> msg;
            while (receivedIds.size() < expectedIds.size() && (msg = consumer.receive(10, TimeUnit.SECONDS)) != null) {
                // the writer emits one (possibly quoted) "<id>,<reading>" per record; the id is what precedes the padding
                final String value = new String(msg.getValue(), UTF_8).replace("\"", "");
                receivedIds.add(value.substring(0, value.indexOf('x')));
                consumer.acknowledge(msg);
            }
        }
        assertEquals(mode + ": every record must have reached the topic exactly through the one producer",
                expectedIds, receivedIds);
    }
}
