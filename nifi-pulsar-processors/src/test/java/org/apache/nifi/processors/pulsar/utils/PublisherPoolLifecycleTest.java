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
package org.apache.nifi.processors.pulsar.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.nifi.util.MockComponentLog;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Before;
import org.junit.Test;

/**
 * Lifecycle contract for {@link PublisherPool}.
 * <p>
 * The pool never pooled and never closed anything: {@code publisherQueue} was only ever read from, so
 * {@code obtainPublisher()} built a fresh Pulsar producer on every call, a lease's {@code close()} hit
 * {@code publisherQueue.remove(this)} on a permanently empty queue instead of closing the producer, and
 * {@code PublisherPool.close()} drained that same empty queue. The net effect was that every producer the
 * bundle opened - and its broker connection - was leaked.
 */
public class PublisherPoolLifecycleTest {

    private static final String TOPIC_A = "persistent://public/default/alpha";
    private static final String TOPIC_B = "persistent://public/default/beta";

    private PulsarClient client;
    private List<Producer<?>> created;

    @Before
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void init() throws Exception {
        created = new ArrayList<>();
        final String[] lastTopic = new String[1];

        final ProducerBuilder builder = mock(ProducerBuilder.class);
        when(builder.topic(anyString())).thenAnswer(invocation -> {
            lastTopic[0] = invocation.getArgument(0);
            return builder;
        });
        when(builder.loadConf(any())).thenReturn(builder);
        // stubbed once: re-stubbing create() inside an answer would invoke it and build a stray producer
        when(builder.create()).thenAnswer(invocation -> {
            final Producer producer = mock(Producer.class);
            when(producer.getTopic()).thenReturn(lastTopic[0]);
            when(producer.newMessage()).thenAnswer(m -> messageBuilder());
            created.add(producer);
            return producer;
        });

        client = mock(PulsarClient.class);
        when(client.newProducer()).thenReturn(builder);
    }

    /** Enough of the fluent send chain for publish() to run against a mock producer. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TypedMessageBuilder messageBuilder() throws Exception {
        final TypedMessageBuilder tmb = mock(TypedMessageBuilder.class);
        when(tmb.properties(any())).thenReturn(tmb);
        when(tmb.value(any())).thenReturn(tmb);
        when(tmb.key(any())).thenReturn(tmb);
        when(tmb.send()).thenReturn(mock(MessageId.class));
        return tmb;
    }

    private PublisherPool newPool() {
        return new PublisherPool(new MockComponentLog("id", new Object()), new HashMap<>(), client);
    }

    /** Releasing a lease while the pool is open must return it for reuse, not silently drop it. */
    @Test
    public void aReleasedLeaseIsReusedForTheSameTopic() {
        final PublisherPool pool = newPool();

        final PublisherLease first = pool.obtainPublisher(TOPIC_A);
        assertNotNull(first);
        first.close();

        final PublisherLease second = pool.obtainPublisher(TOPIC_A);
        assertSame("a released lease should be handed back out rather than a new producer built", first, second);
        assertEquals("only one producer should have been created for one topic", 1, created.size());
    }

    /** Leases are bound to a producer for one topic, so they must never be handed to a different topic. */
    @Test
    public void aLeaseIsNeverReusedForADifferentTopic() {
        final PublisherPool pool = newPool();

        final PublisherLease alpha = pool.obtainPublisher(TOPIC_A);
        alpha.close();

        final PublisherLease beta = pool.obtainPublisher(TOPIC_B);
        assertEquals("the beta lease must not be the alpha producer", TOPIC_B, beta.getTopicName());
        assertEquals(2, created.size());
    }

    /** Closing the pool must close every producer it handed out, including the ones returned to it. */
    @Test
    public void closingThePoolClosesEveryProducer() throws Exception {
        final PublisherPool pool = newPool();

        pool.obtainPublisher(TOPIC_A).close();
        pool.obtainPublisher(TOPIC_B).close();
        assertEquals(2, created.size());

        pool.close();

        for (final Producer<?> producer : created) {
            verify(producer, times(1)).close();
        }
    }

    /** A lease still checked out when the pool closes must not outlive it. */
    @Test
    public void aLeaseClosedAfterThePoolClosesItsProducer() throws Exception {
        final PublisherPool pool = newPool();

        final PublisherLease lease = pool.obtainPublisher(TOPIC_A);
        pool.close();
        lease.close();

        verify(created.get(0), times(1)).close();
    }

    /**
     * The message counter is cumulative for the lifetime of a lease, so a reused lease has to start from
     * zero again - otherwise PublishPulsarRecord's msg.count attribute over-reports on the second FlowFile
     * and every one after it.
     */
    @Test
    public void aReusedLeaseStartsCountingFromZero() throws Exception {
        final PublisherPool pool = newPool();

        final PublisherLease lease = pool.obtainPublisher(TOPIC_A);
        lease.publish(new org.apache.nifi.util.MockFlowFile(1L),
                new java.io.ByteArrayInputStream("a\nb\nc".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                null, new HashMap<>(), "\n".getBytes(java.nio.charset.StandardCharsets.UTF_8), false);
        assertEquals(3, lease.complete());
        lease.close();

        final PublisherLease reused = pool.obtainPublisher(TOPIC_A);
        assertSame(lease, reused);
        assertEquals("a lease handed back out must not carry the previous FlowFile's count", 0, reused.complete());
    }
}
