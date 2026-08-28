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

import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.StandardPulsarClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for tests that exercise the processors against a real Pulsar broker started in Docker.
 * <p>
 * The unit tests in this module all run against a mocked {@code PulsarClientService}, which means the
 * processors have never been executed against a real broker: anything that depends on genuine broker
 * behaviour - acknowledgement semantics, subscription types, partitioned topics, schemas - is currently
 * unverified. These integration tests close that gap.
 * <p>
 * One broker is started per test class ({@code @ClassRule}) and shared by its tests, since container
 * startup dominates the runtime. The image is pinned by the {@code pulsar.image} property in the parent
 * pom so it stays in step with the Pulsar client version the bundle builds against.
 * <p>
 * These are named {@code *IT} so Surefire ignores them: {@code mvn test} and {@code mvn package} stay
 * fast and need no Docker. They run from {@code mvn verify}.
 */
public abstract class AbstractPulsarIT {

    /** Overridable so the image can be pinned from the build; falls back to the client version. */
    private static final DockerImageName PULSAR_IMAGE = DockerImageName
            .parse(System.getProperty("pulsar.image", "apachepulsar/pulsar:4.2.4"))
            .asCompatibleSubstituteFor("apachepulsar/pulsar");

    @ClassRule
    public static final PulsarContainer PULSAR = new PulsarContainer(PULSAR_IMAGE)
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private static PulsarClient client;

    @BeforeClass
    public static void startClient() throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(PULSAR.getPulsarBrokerUrl()).build();
    }

    @AfterClass
    public static void stopClient() throws PulsarClientException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    /** A client connected to the containerised broker, for arranging and asserting outside the processors. */
    protected static PulsarClient getClient() {
        return client;
    }

    protected static String getBrokerUrl() {
        return PULSAR.getPulsarBrokerUrl();
    }

    /**
     * Registers a real {@link StandardPulsarClientService} pointed at the container and wires it into the
     * runner, so the processor under test talks to an actual broker instead of a Mockito mock.
     *
     * @param runner the runner for the processor under test
     * @param identifier the controller service id to register under
     */
    protected void addRealPulsarClientService(final TestRunner runner, final String identifier)
            throws InitializationException {
        final ControllerService service = new StandardPulsarClientService();
        runner.addControllerService(identifier, service);
        runner.setProperty(service, StandardPulsarClientService.PULSAR_SERVICE_URL, getBrokerUrl());
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    /** Publishes messages to a topic using a plain client, independent of the processors. */
    protected void publish(final String topic, final String... messages) throws PulsarClientException {
        try (Producer<byte[]> producer = getClient().newProducer(Schema.BYTES).topic(topic).create()) {
            for (final String message : messages) {
                producer.send(message.getBytes());
            }
        }
    }

    /** Waits until {@code condition} holds, so tests do not depend on broker timing. */
    protected static void await(final String description, final java.util.concurrent.Callable<Boolean> condition)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

        while (System.nanoTime() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(250);
        }

        throw new AssertionError("Timed out after 30s waiting for: " + description);
    }
}
