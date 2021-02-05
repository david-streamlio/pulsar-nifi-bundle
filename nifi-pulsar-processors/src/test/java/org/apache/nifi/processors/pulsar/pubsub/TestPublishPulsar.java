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

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;

import org.apache.pulsar.client.api.Producer;

import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPublishPulsar extends AbstractPulsarProcessorTest<byte[]> {

    @Mock
    protected Producer<byte[]> mockProducer;

    @Before
    public void init() throws InitializationException {
        mockProducer = mock(Producer.class);
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addPulsarClientService();
    }

    protected void doMappedPropertiesTest() throws UnsupportedEncodingException {
        mockClientService.setMockProducer(mockProducer);

        final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("prop", "val");

        runner.setProperty(PublishPulsar.TOPIC, "my-topic");
        runner.setProperty(PublishPulsar.MAPPED_MESSAGE_PROPERTIES, "prop");
        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        Map<String, String> expectedProperties = new HashMap<String, String>();
        expectedProperties.put("prop", "val");
        // Verify that we sent the data to topic-b.
        verify(mockClientService.getMockTypedMessageBuilder()).properties(expectedProperties);
    }

    protected void doMessageKeyTest() throws UnsupportedEncodingException {
        mockClientService.setMockProducer(mockProducer);

        final String content = "some content";
        Map<String, String> attributes1 = new HashMap<String, String>();
        attributes1.put("prop", "val");

        Map<String, String> attributes2 = new HashMap<String, String>();

        runner.setProperty(PublishPulsar.TOPIC, "my-topic");
        runner.setProperty(PublishPulsar.MESSAGE_KEY, "${prop}");
        runner.enqueue(content.getBytes("UTF-8"), attributes1 );
        runner.enqueue(content.getBytes("UTF-8"), attributes2 );

        runner.run(2);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        // Verify that we sent the data to topic-b.
        verify(mockClientService.getMockTypedMessageBuilder(), times(1)).key("val");
    }

    @Test
    public void dynamicTopicTest() throws UnsupportedEncodingException, PulsarClientException {
        when(mockProducer.getTopic()).thenReturn("topic-b");
        mockClientService.setMockProducer(mockProducer);

        runner.setProperty(PublishPulsar.TOPIC, "${topic}");

        final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("topic", "topic-b");

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        // Verify that we sent the data to topic-b.
        verify(mockClientService.getMockProducerBuilder(), times(1)).topic("topic-b");
    }
}
