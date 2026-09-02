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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.logging.ComponentLog;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Regression tests for the producer leak: the pool must hand out idle producers again, and closing the pool
 * must close every producer it created.
 */
@SuppressWarnings("unchecked")
public class PublisherPoolTest {

    private PulsarClient client;
    private ProducerBuilder<byte[]> builder;
    private final List<Producer<byte[]>> producers = new ArrayList<>();
    private final List<String> requestedTopics = new ArrayList<>();
    private PublisherPool pool;

    @Before
    public void setUp() throws PulsarClientException {
        client = mock(PulsarClient.class);
        builder = mock(ProducerBuilder.class, RETURNS_SELF);
        // the pool creates producers with AUTO_PRODUCE_BYTES so the broker validates payloads against the
        // topic's schema, so it is newProducer(Schema) that has to be stubbed
        when(client.newProducer(any(Schema.class))).thenReturn(builder);
        // doAnswer(...).when(...) so that (re)stubbing never invokes a previous answer
        doAnswer(invocation -> {
            requestedTopics.add(invocation.getArgument(0));
            return builder;
        }).when(builder).topic(anyString());
        doAnswer(invocation -> {
            final Producer<byte[]> producer = mock(Producer.class);
            when(producer.getTopic()).thenReturn(requestedTopics.get(requestedTopics.size() - 1));
            producers.add(producer);
            return producer;
        }).when(builder).create();

        pool = new PublisherPool(mock(ComponentLog.class), Collections.emptyMap(), client);
    }

    /**
     * The Batch Builder has to reach the producer through the builder, because it cannot reach it through
     * the configuration map: {@code loadConf} serialises that map through JSON, and BatcherBuilder is an
     * interface with no serialisable state, so a builder placed there is dropped without an error and the
     * producer batches with the default one. This is the link nothing else covers - the processor's own test
     * proves only which builder it chose, not that the pool passes it on.
     */
    @Test
    public void theBatchBuilderIsSetOnTheProducerBuilder() {
        final PublisherPool keyBased = new PublisherPool(
                mock(ComponentLog.class), Collections.emptyMap(), client, BatcherBuilder.KEY_BASED);

        keyBased.obtainPublisher("persistent://public/default/key-based");

        verify(builder).batcherBuilder(BatcherBuilder.KEY_BASED);
    }

    /** No builder chosen means the client keeps its own default, not a null handed to the builder. */
    @Test
    public void noBatchBuilderIsSetWhenNoneWasChosen() {
        pool.obtainPublisher("persistent://public/default/no-batcher");

        verify(builder, never()).batcherBuilder(any());
    }

    /**
     * Producers must be created with AUTO_PRODUCE_BYTES so the broker validates each payload against the
     * schema the topic currently carries. With the default BYTES schema a topic with, say, an AVRO schema
     * accepted arbitrary content: the message landed looking valid and every schema-aware consumer then
     * failed to decode it. This pins the schema choice, which is otherwise invisible from the outside.
     */
    @Test
    public void producersValidateAgainstTheTopicSchema() {
        pool.obtainPublisher("persistent://public/default/schema-check");

        final ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);
        verify(client).newProducer(schema.capture());

        // getSchemaInfo() throws until the schema is bound to a topic, so identify it by what it is
        assertNotSame("producers must not write opaque bytes past the topic's schema",
                Schema.BYTES, schema.getValue());
        assertTrue("expected an AUTO_PRODUCE_BYTES schema so the broker validates the payload, but got "
                        + schema.getValue().getClass().getName(),
                schema.getValue().getClass().getSimpleName().contains("AutoProduceBytes"));
    }

    @Test
    public void closingALeaseReturnsTheProducerToThePool() throws PulsarClientException {
        final PublisherLease first = pool.obtainPublisher("topic-a");
        first.close();
        final PublisherLease second = pool.obtainPublisher("topic-a");

        assertSame("an idle lease for the same topic must be handed out again", first, second);
        verify(builder, times(1)).create();
        verify(producers.get(0), never()).close();
        assertEquals(1, pool.getOpenProducerCount());
        assertEquals(0, pool.getIdleProducerCount());
    }

    @Test
    public void aNewProducerIsCreatedOnlyWhenEveryLeaseForTheTopicIsInUse() throws PulsarClientException {
        final PublisherLease first = pool.obtainPublisher("topic-a");
        final PublisherLease second = pool.obtainPublisher("topic-a");

        assertNotSame(first, second);
        verify(builder, times(2)).create();

        first.close();
        second.close();
        assertEquals(2, pool.getIdleProducerCount());

        pool.obtainPublisher("topic-a");
        pool.obtainPublisher("topic-a");
        verify(builder, times(2)).create();
        assertEquals(0, pool.getIdleProducerCount());
        assertEquals(2, pool.getOpenProducerCount());
    }

    @Test
    public void producersArePooledPerTopic() throws PulsarClientException {
        pool.obtainPublisher("topic-a").close();
        final PublisherLease other = pool.obtainPublisher("topic-b");

        verify(builder, times(2)).create();
        assertEquals("topic-b", other.getTopicName());
        assertEquals(1, pool.getIdleProducerCount());

        assertEquals("topic-a", pool.obtainPublisher("topic-a").getTopicName());
        verify(builder, times(2)).create();
    }

    @Test
    public void closingThePoolClosesIdleAndInUseProducersExactlyOnce() throws PulsarClientException {
        final PublisherLease inUse = pool.obtainPublisher("topic-a");
        pool.obtainPublisher("topic-b").close();
        assertEquals(2, pool.getOpenProducerCount());

        pool.close();

        for (Producer<byte[]> producer : producers) {
            verify(producer, times(1)).flush();
            verify(producer, times(1)).close();
        }
        assertEquals(0, pool.getOpenProducerCount());
        assertEquals(0, pool.getIdleProducerCount());
        assertTrue(pool.isClosed());

        // the lease that was in use is closed by its owner later: its producer must not be closed twice
        inUse.close();
        verify(producers.get(0), times(1)).close();
    }

    @Test
    public void aLeaseClosedAfterThePoolClosesItsProducerInsteadOfReturning() throws PulsarClientException {
        final PublisherLease lease = pool.obtainPublisher("topic-a");
        pool.close();
        verify(producers.get(0), times(1)).close();

        lease.close();
        lease.close();
        verify(producers.get(0), times(1)).close();
        assertEquals(0, pool.getIdleProducerCount());
    }

    @Test
    public void obtainAfterCloseIsRejected() {
        pool.close();
        try {
            pool.obtainPublisher("topic-a");
            fail("expected IllegalStateException");
        } catch (final IllegalStateException expected) {
            // ok
        }
    }

    @Test
    public void blankTopicYieldsNoProducer() throws PulsarClientException {
        assertNull(pool.obtainPublisher(""));
        assertNull(pool.obtainPublisher(null));
        verify(builder, never()).create();
        assertEquals(0, pool.getOpenProducerCount());
    }

    @Test
    public void producerCreationFailureYieldsNullAndLeaksNothing() throws PulsarClientException {
        doThrow(new PulsarClientException("cannot create")).when(builder).create();

        assertNull(pool.obtainPublisher("topic-a"));
        assertEquals(0, pool.getOpenProducerCount());
        assertEquals(0, pool.getIdleProducerCount());
    }
}
