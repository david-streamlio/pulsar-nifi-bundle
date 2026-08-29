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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * Messages from one FlowFile must reach the producer in the order they appear in it (issue #173).
 * <p>
 * {@code PublisherLease.send()} wrapped a blocking {@code tmb.send()} in
 * {@code CompletableFuture.supplyAsync()}, handing every message to the common ForkJoinPool, so the sends
 * raced and the broker received them in whatever order the pool happened to run them. Observed against a
 * real broker while testing #34: a FlowFile holding sensor-2 then sensor-3 arrived as sensor-3, sensor-2.
 * <p>
 * Pulsar preserves ordering per producer, and flows rely on it - ordered readings from one device, change
 * events for one entity. It also quietly defeats message keys, which exist to order messages sharing a key.
 * <p>
 * The asynchronous path was never affected: it calls {@code tmb.sendAsync()} on the calling thread, and
 * Pulsar preserves the order those calls are made in.
 */
public class PublishPulsarOrderingTest extends AbstractPulsarProcessorTest<GenericRecord> {

    /** How long the first send is held up, long enough for a racing send to overtake it. */
    private static final long FIRST_SEND_DELAY_MILLIS = 400L;

    /** The order in which message bodies actually reached the producer. */
    private final List<String> published = Collections.synchronizedList(new java.util.ArrayList<>());

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "ordering");
        runner.setProperty(AbstractPulsarProducerProcessor.MESSAGE_DEMARCATOR, "\n");

        final AtomicInteger seen = new AtomicInteger();

        // Hold the first message up. If the sends run concurrently the later ones overtake it and the
        // recorded order differs from the FlowFile's; if they run in order, nothing can overtake.
        doAnswer(invocation -> {
            if (seen.getAndIncrement() == 0) {
                Thread.sleep(FIRST_SEND_DELAY_MILLIS);
            }
            return mockClientService.getMockTypedMessageBuilder();
        }).when(mockClientService.getMockProducer()).newMessage();

        doAnswer(invocation -> {
            published.add(new String((byte[]) invocation.getArgument(0), UTF_8));
            return mockClientService.getMockTypedMessageBuilder();
        }).when(mockClientService.getMockTypedMessageBuilder()).value(any());
    }

    /** Synchronous mode: the path that dispatched each send to a thread pool. */
    @Test
    public void messagesArePublishedInFlowFileOrderWhenSynchronous() {
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "false");

        runner.enqueue("m1\nm2\nm3".getBytes(UTF_8));
        runner.run(1, true);

        assertEquals("messages reached the producer out of order", Arrays.asList("m1", "m2", "m3"), published);
    }

    /** Asynchronous mode, which issues sendAsync on the calling thread and was already ordered. */
    @Test
    public void messagesArePublishedInFlowFileOrderWhenAsynchronous() {
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, "true");

        runner.enqueue("m1\nm2\nm3".getBytes(UTF_8));
        runner.run(1, true);

        assertEquals("messages reached the producer out of order", Arrays.asList("m1", "m2", "m3"), published);
    }
}
