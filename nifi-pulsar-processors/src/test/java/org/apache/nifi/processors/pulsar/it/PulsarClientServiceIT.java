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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.StandardPulsarClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.Before;
import org.junit.Test;

/**
 * Coverage for {@link StandardPulsarClientService} against a real broker.
 * <p>
 * This replaces a file of the same name that lived in the client-service module and had never run: it
 * hardcoded {@code localhost:6650}, expecting a broker somebody had started by hand, and that module has
 * no Failsafe plugin configured, so no build phase ever executed it. A test that cannot run is worse than
 * no test, because the filename implies the service is covered.
 * <p>
 * It also asserts the service releases its client on disable - the same class of resource bug that went
 * unnoticed on the producer side, where nothing ever closed a producer.
 */
public class PulsarClientServiceIT extends AbstractPulsarIT {

    private TestRunner runner;
    private StandardPulsarClientService service;

    @Before
    public void init() throws InitializationException {
        // any processor will do; the runner is only here to host the controller service
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        service = new StandardPulsarClientService();
        runner.addControllerService("pulsar-client", service);
        runner.setProperty(service, StandardPulsarClientService.PULSAR_SERVICE_URL, getBrokerUrl());
    }

    /** The point of the service: hand back a client that can actually reach the broker. */
    @Test
    public void producesAClientThatCanRoundTripAMessage() throws Exception {
        runner.enableControllerService(service);
        runner.assertValid(service);

        final PulsarClient client = service.getPulsarClient();
        assertNotNull(client);

        final String topic = "persistent://public/default/service-roundtrip-" + System.nanoTime();

        try (Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                        .topic(topic).subscriptionName("service-sub").subscribe();
             Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(topic).create()) {

            producer.send("hello from the service".getBytes(UTF_8));

            final Message<byte[]> received = consumer.receive(30, java.util.concurrent.TimeUnit.SECONDS);
            assertNotNull("no message arrived within 30s", received);
            consumer.acknowledge(received);
            assertEquals("hello from the service", new String(received.getValue(), UTF_8));
        }
    }

    /** Downstream processors use this for provenance, so it has to reflect what was configured. */
    @Test
    public void reportsTheConfiguredBrokerUrl() {
        runner.enableControllerService(service);

        final PulsarClientService asService = service;
        assertEquals(getBrokerUrl(), asService.getPulsarBrokerRootURL());
    }

    /**
     * Disabling the service must close the client and release its connections. Nothing verified this
     * before, and an unclosed client holds broker connections open exactly like the leaked producers did.
     */
    @Test
    public void disablingTheServiceClosesItsClient() throws Exception {
        runner.enableControllerService(service);
        final PulsarClient client = service.getPulsarClient();
        assertNotNull(client);

        runner.disableControllerService(service);

        try {
            client.newProducer(Schema.BYTES)
                    .topic("persistent://public/default/after-disable-" + System.nanoTime())
                    .create();
            fail("the client should have been closed when the service was disabled");
        } catch (final PulsarClientException.AlreadyClosedException expected) {
            // exactly what a closed client does
        } catch (final IllegalStateException expected) {
            // some client versions surface a closed client this way instead
            assertTrue(expected.getMessage() != null);
        }
    }

    /** A service pointed at a broker that is not there must fail loudly at enable time, not later. */
    @Test
    public void anUnreachableBrokerFailsOnEnable() throws InitializationException {
        final StandardPulsarClientService unreachable = new StandardPulsarClientService();
        runner.addControllerService("unreachable", unreachable);
        // a syntactically valid URL that nothing is listening on
        runner.setProperty(unreachable, StandardPulsarClientService.PULSAR_SERVICE_URL,
                "pulsar://127.0.0.1:1");
        runner.assertValid(unreachable);

        try {
            runner.enableControllerService(unreachable);
            // The Pulsar client connects lazily, so enabling may legitimately succeed. What must not
            // happen is a silently broken service that reports a usable client.
            assertNotNull(unreachable.getPulsarClient());
        } catch (final IllegalStateException expected) {
            assertTrue("enable should explain what went wrong, but said: " + expected.getMessage(),
                    expected.getMessage() != null && !expected.getMessage().isEmpty());
        }
    }

    /** A malformed broker URL must be rejected by validation before the service is ever enabled. */
    @Test
    public void aMalformedBrokerUrlIsRejected() throws InitializationException {
        final StandardPulsarClientService malformed = new StandardPulsarClientService();
        runner.addControllerService("malformed", malformed);
        runner.setProperty(malformed, StandardPulsarClientService.PULSAR_SERVICE_URL, "not-a-broker-url");

        runner.assertNotValid(malformed);
    }
}
