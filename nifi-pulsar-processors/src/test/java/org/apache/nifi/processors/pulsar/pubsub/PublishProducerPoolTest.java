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

import static org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord.RECORD_READER;
import static org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord.RECORD_WRITER;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

/**
 * The publishers must reuse one producer per topic across FlowFiles and close it when the processor stops.
 * Before the PublisherPool fix PublishPulsarRecord opened one producer per FlowFile and neither processor ever
 * closed a producer.
 */
public class PublishProducerPoolTest extends AbstractPulsarProcessorTest<byte[]> {

    private static final String TOPIC = "pool-topic";

    private void initRecordRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);

        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService("record-reader", readerService);
        runner.enableControllerService(readerService);

        final MockRecordWriter writerService = new MockRecordWriter("name, age");
        runner.addControllerService("record-writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(RECORD_READER, "record-reader");
        runner.setProperty(RECORD_WRITER, "record-writer");
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC);
    }

    @Test
    public void publishPulsarRecordReusesOneProducerPerTopicAndClosesItOnStop() throws InitializationException, PulsarClientException {
        initRecordRunner();

        for (int i = 0; i < 3; i++) {
            runner.enqueue("Mary Jane, 32\nJohn Doe, 40".getBytes(StandardCharsets.UTF_8));
        }
        runner.run(3, true);

        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS, 3);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PublishPulsarRecord.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            // the lease is reused, so the count must be per FlowFile rather than cumulative
            flowFile.assertAttributeEquals(AbstractPulsarProducerProcessor.MSG_COUNT, "2");
            flowFile.assertAttributeEquals(AbstractPulsarProducerProcessor.TOPIC_NAME, TOPIC);
        }

        // one producer for the three FlowFiles, closed exactly once when the processor stopped
        verify(mockClientService.getMockProducerBuilder(), times(1)).create();
        verify(mockClientService.getMockProducer(), times(1)).close();

        // a restart builds a new pool and therefore a new producer
        runner.enqueue("Mary Jane, 32".getBytes(StandardCharsets.UTF_8));
        runner.run(1, true);
        verify(mockClientService.getMockProducerBuilder(), times(2)).create();
        verify(mockClientService.getMockProducer(), times(2)).close();
    }

    @Test
    public void publishPulsarReusesOneProducerPerTopicAndClosesItOnStop() throws InitializationException, PulsarClientException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC);

        for (int i = 0; i < 3; i++) {
            runner.enqueue("message".getBytes(StandardCharsets.UTF_8));
        }
        runner.run(3, true);

        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 3);
        verify(mockClientService.getMockProducerBuilder(), times(1)).create();
        verify(mockClientService.getMockProducer(), times(1)).close();
    }
}
