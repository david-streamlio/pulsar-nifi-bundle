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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.processors.pulsar.utils.PublisherPool;
import org.apache.nifi.processors.pulsar.utils.PublisherUnavailableException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

/**
 * Under an exclusive access mode a topic has one producer, and a task that finds it held by another task waits a
 * bounded time for it. When the wait runs out nothing has been attempted for the FlowFile, so it must not go to
 * {@code failure} - that is where FlowFiles the broker refused go. It goes back to the queue for a later trigger,
 * with the rest of the batch, and the processor yields so the retry is not immediate (#219).
 */
public class PublishPulsarHeldProducerTest extends AbstractPulsarProcessorTest<byte[]> {

    private void runWithAHeldProducer(final Processor publisher) throws InitializationException {
        runner = TestRunners.newTestRunner(publisher);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "held-topic");
        if (publisher instanceof PublishPulsarRecord) {
            final MockRecordParser reader = new MockRecordParser();
            reader.addSchemaField("name", RecordFieldType.STRING);
            runner.addControllerService("reader", reader);
            runner.enableControllerService(reader);
            final MockRecordWriter writer = new MockRecordWriter("name");
            runner.addControllerService("writer", writer);
            runner.enableControllerService(writer);
            runner.setProperty(PublishPulsarRecord.RECORD_READER, "reader");
            runner.setProperty(PublishPulsarRecord.RECORD_WRITER, "writer");
        }

        runner.enqueue("first");
        runner.enqueue("second");
        runner.enqueue("third");
        runner.run(1, false, true);
    }

    private static PublisherPool aPoolWhoseProducerIsHeld() {
        final PublisherPool pool = mock(PublisherPool.class);
        when(pool.obtainPublisher(anyString()))
                .thenThrow(new PublisherUnavailableException("The exclusive producer on topic held-topic was still held by another task after 5000 ms"));
        return pool;
    }

    private void assertNothingFailedAndEverythingIsBackInTheQueue() {
        runner.assertTransferCount(AbstractPulsarProducerProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPulsarProducerProcessor.REL_SUCCESS, 0);
        assertEquals("every FlowFile of the batch goes back to the queue, since none was attempted",
                3, runner.getQueueSize().getObjectCount());
        assertTrue("the processor yields so the next trigger does not spin on the held producer", runner.isYieldCalled());
        assertEquals(1, runner.getLogger().getWarnMessages().size());
        assertEquals(0, runner.getLogger().getErrorMessages().size());
    }

    @Test
    public void publishPulsarReturnsTheBatchToTheQueueAndYields() throws InitializationException {
        runWithAHeldProducer(new PublishPulsar() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return aPoolWhoseProducerIsHeld();
            }
        });

        assertNothingFailedAndEverythingIsBackInTheQueue();
    }

    @Test
    public void publishPulsarRecordReturnsTheBatchToTheQueueAndYields() throws InitializationException {
        runWithAHeldProducer(new PublishPulsarRecord() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return aPoolWhoseProducerIsHeld();
            }
        });

        assertNothingFailedAndEverythingIsBackInTheQueue();
    }
}
