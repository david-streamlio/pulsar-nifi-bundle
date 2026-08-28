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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Producer lifecycle against a real broker.
 * <p>
 * The publish path had no real-broker coverage at all, which is how it went unnoticed that
 * {@code PublisherPool} never pooled and never closed anything: leases were returned to a queue nothing
 * ever added to, so every producer the bundle opened - and its broker connection - was leaked. These
 * tests assert against what the broker itself reports, which is the only place that leak is visible.
 */
public class PublishPulsarLifecycleIT extends AbstractPulsarIT {

    /** Pulsar reports connected producers per topic in its stats endpoint. */
    private static final Pattern PRODUCER_NAME = Pattern.compile("\"producerName\"");

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");
    }

    /**
     * The headline leak: many FlowFiles must not mean many producers. Before the fix each obtained lease
     * was abandoned rather than returned, so the connected-producer count tracked the FlowFile count.
     */
    @Test
    public void producerCountDoesNotGrowWithFlowFiles() throws Exception {
        final String topic = topic("producer-count");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);

        for (int n = 1; n <= 25; n++) {
            runner.enqueue(("message-" + n).getBytes());
        }
        runner.run(25, false);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 25);

        final int producers = connectedProducers(topic);
        assertTrue("25 FlowFiles opened " + producers + " producers on the broker; a pool should hold one "
                + "per topic, not one per FlowFile", producers <= 2);
    }

    /** Stopping the processor must hand every producer back to the broker, not abandon it. */
    @Test
    public void stoppingTheProcessorClosesItsProducers() throws Exception {
        final String topic = topic("producer-close");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);

        runner.enqueue("hello".getBytes());
        runner.run(1, false);
        assertTrue("expected a producer while running", connectedProducers(topic) >= 1);

        // stopOnFinish runs @OnUnscheduled and @OnStopped, which is where the pool is closed
        runner.run(1, true);

        await("the broker to report no connected producers", () -> connectedProducers(topic) == 0);
        assertEquals(0, connectedProducers(topic));
    }

    /** Restarting repeatedly must not accumulate producers from previous runs. */
    @Test
    public void restartingDoesNotAccumulateProducers() throws Exception {
        final String topic = topic("producer-restart");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, topic);

        for (int round = 1; round <= 3; round++) {
            runner.enqueue(("round-" + round).getBytes());
            runner.run(1, true);
            await("producers released after round " + round, () -> connectedProducers(topic) == 0);
        }

        assertEquals("three start/stop rounds should leave nothing connected", 0, connectedProducers(topic));
    }

    /** Publishing to several topics keeps one producer each, not one per FlowFile per topic. */
    @Test
    public void eachTopicKeepsASingleProducer() throws Exception {
        final String topic = topic("multi");
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "${target.topic}");

        for (int n = 1; n <= 12; n++) {
            runner.enqueue(("message-" + n).getBytes(),
                    java.util.Collections.singletonMap("target.topic", topic + "-" + (n % 3)));
        }
        runner.run(12, false);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS, 12);

        for (int partition = 0; partition < 3; partition++) {
            final int producers = connectedProducers(topic + "-" + partition);
            assertTrue("topic " + partition + " has " + producers + " producers for 4 FlowFiles",
                    producers <= 2);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static String topic(final String name) {
        return "persistent://public/default/" + name + "-" + System.nanoTime();
    }

    /**
     * Asks the broker how many producers are currently connected to a topic. Uses the admin REST endpoint
     * directly so the test does not depend on an admin client version that may drift from the client.
     */
    private static int connectedProducers(final String topic) throws Exception {
        final String shortName = topic.substring(topic.lastIndexOf('/') + 1);
        final String url = PULSAR.getHttpServiceUrl()
                + "/admin/v2/persistent/public/default/" + shortName + "/stats";

        final HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 404) {
            return 0;   // topic not created yet, so nothing is connected to it
        }

        if (response.statusCode() != 200) {
            throw new AssertionError("Unable to read topic stats (" + response.statusCode() + "): " + response.body());
        }

        final Matcher matcher = PRODUCER_NAME.matcher(response.body());
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
