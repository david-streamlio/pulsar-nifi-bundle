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
package org.apache.nifi.processors.pulsar.pubsub.sync;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarClientService;

import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord.RECORD_READER;
import static org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord.RECORD_WRITER;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestSyncPublishJsonRecordAsBytes {

    protected static final String TOPIC_NAME = "json-unit-test";

    protected TestRunner runner;

    protected MockPulsarClientService mockClientService;

    @Before
    public void setup() throws InitializationException {

        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);

        final String readerId = "record-reader";
        final JsonTreeReader readerService = new JsonTreeReader();
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);

        final String writerId = "record-writer";
        final JsonRecordSetWriter writerService = new JsonRecordSetWriter();
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(RECORD_READER, readerId);
        runner.setProperty(RECORD_WRITER, writerId);

        mockClientService = new MockPulsarClientService();
        runner.addControllerService("Pulsar Client Service", mockClientService);
        runner.enableControllerService(mockClientService);
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "Pulsar Client Service");
    }

    @Test
    public void testSingleRecordSuccess() throws PulsarClientException {
        final String content = "{" +
                "  \"name\": \"John Doe\"," +
                "  \"age\": 30," +
                "  \"email\": \"johndoe@example.com\"," +
                "  \"address\": {" +
                "    \"street\": \"123 Main Street\"," +
                "    \"city\": \"New York\"," +
                "    \"state\": \"NY\"," +
                "    \"postalCode\": \"10001\"" +
                "  }," +
                "  \"phoneNumbers\": [" +
                "    \"123-456-7890\"," +
                "    \"987-654-3210\"" +
                "  ]," +
                "  \"isEmployed\": true," +
                "  \"languages\": [\"Java\", \"Python\", \"JavaScript\"]" +
                "}";

        runner.enqueue(content);
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS);

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PublishPulsarRecord.REL_SUCCESS);
        MockFlowFile result = results.get(0);

        result.assertAttributeEquals(PublishPulsarRecord.MSG_COUNT, "1");
//        verify(mockClientService.getMockTypedMessageBuilder(), times(1)).value(content.getBytes());
        verify(mockClientService.getMockTypedMessageBuilder(), times(1)).send();
    }

    @Test
    public final void testMultipleRecordSuccess() throws PulsarClientException {
        final String content = "[\n" +
                "  {\n" +
                "    \"name\": \"John Doe\",\n" +
                "    \"age\": 30,\n" +
                "    \"email\": \"johndoe@example.com\",\n" +
                "    \"address\": {\n" +
                "      \"street\": \"123 Main Street\",\n" +
                "      \"city\": \"New York\",\n" +
                "      \"state\": \"NY\",\n" +
                "      \"postalCode\": \"10001\"\n" +
                "    },\n" +
                "    \"phoneNumbers\": [\n" +
                "      \"123-456-7890\",\n" +
                "      \"987-654-3210\"\n" +
                "    ],\n" +
                "    \"isEmployed\": true,\n" +
                "    \"languages\": [\"Java\", \"Python\", \"JavaScript\"]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"Jane Smith\",\n" +
                "    \"age\": 28,\n" +
                "    \"email\": \"janesmith@example.com\",\n" +
                "    \"address\": {\n" +
                "      \"street\": \"456 Elm Street\",\n" +
                "      \"city\": \"San Francisco\",\n" +
                "      \"state\": \"CA\",\n" +
                "      \"postalCode\": \"94101\"\n" +
                "    },\n" +
                "    \"phoneNumbers\": [\n" +
                "      \"555-123-4567\",\n" +
                "      \"555-987-6543\"\n" +
                "    ],\n" +
                "    \"isEmployed\": false,\n" +
                "    \"languages\": [\"C++\", \"Ruby\", \"PHP\"]\n" +
                "  }\n" +
                "]\n";

        runner.enqueue(content);
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS);

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PublishPulsarRecord.REL_SUCCESS);
        MockFlowFile result = results.get(0);

        result.assertAttributeEquals(PublishPulsarRecord.MSG_COUNT, "2");
//        verify(mockClientService.getMockTypedMessageBuilder(), times(1)).value(content.getBytes());
        verify(mockClientService.getMockTypedMessageBuilder(), times(2)).send();
    }
}
