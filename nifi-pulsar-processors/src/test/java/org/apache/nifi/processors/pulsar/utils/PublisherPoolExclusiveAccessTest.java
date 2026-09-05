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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.nifi.logging.ComponentLog;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Under an exclusive <i>Producer Access Mode</i> the pool must hold <b>one</b> producer per topic. The pool
 * otherwise creates a producer for every lease that is in use at the same time, and a second producer on a
 * topic whose first producer is exclusive is exactly what the broker refuses ({@code Exclusive}), fences the
 * first one with ({@code ExclusiveWithFencing}) or parks forever waiting for a producer that never closes
 * ({@code WaitForExclusive}). With more than one concurrent task, {@code PublishPulsarRecord} was competing
 * against its own pooled producers (#219).
 * <p>
 * So a second caller for the same topic waits until the topic's lease is returned and is then handed that same
 * lease, and the mocked builder sees exactly one {@code create()}.
 */
public class PublisherPoolExclusiveAccessTest {

    private static final String TOPIC = "persistent://public/default/exclusive";

    private PulsarClient client;
    private ProducerBuilder<byte[]> builder;
    private final ExecutorService otherTask = Executors.newSingleThreadExecutor();

    @Before
    public void setUp() throws PulsarClientException {
        client = mock(PulsarClient.class);
        builder = mock(ProducerBuilder.class, RETURNS_SELF);
        when(client.newProducer(any(Schema.class))).thenReturn(builder);
        doAnswer(invocation -> builder).when(builder).topic(anyString());
        doAnswer(invocation -> {
            final Producer<byte[]> producer = mock(Producer.class);
            when(producer.getTopic()).thenReturn(TOPIC);
            return producer;
        }).when(builder).create();
    }

    @After
    public void tearDown() {
        otherTask.shutdownNow();
    }

    private PublisherPool poolWith(final ProducerAccessMode accessMode) {
        return new PublisherPool(mock(ComponentLog.class), Map.of("accessMode", accessMode), client);
    }

    /**
     * The invariant for every exclusive mode: however many tasks ask for the topic at once, the broker sees one
     * producer, and the second task gets that producer once the first is done with it.
     */
    @Test
    public void aSecondTaskWaitsForTheTopicsOnlyProducerInsteadOfCreatingAnother() throws Exception {
        for (final ProducerAccessMode mode : new ProducerAccessMode[] {
                ProducerAccessMode.Exclusive, ProducerAccessMode.WaitForExclusive, ProducerAccessMode.ExclusiveWithFencing}) {
            setUp();
            final PublisherPool pool = poolWith(mode);

            final PublisherLease held = pool.obtainPublisher(TOPIC);
            final CompletableFuture<PublisherLease> waiting = CompletableFuture.supplyAsync(() -> pool.obtainPublisher(TOPIC), otherTask);

            try {
                waiting.get(300, TimeUnit.MILLISECONDS);
                throw new AssertionError(mode + ": the second task was handed a lease while the topic's producer was "
                        + "still in use - a second producer was created on an exclusive topic");
            } catch (final TimeoutException expected) {
                // the second task is waiting, as it should be
            }
            verify(builder, times(1)).create();

            held.close();
            final PublisherLease handedOver = waiting.get(5, TimeUnit.SECONDS);

            assertSame(mode + ": the waiting task must receive the lease that was just returned", held, handedOver);
            verify(builder, times(1)).create();
            assertEquals(mode.toString(), 1, pool.getOpenProducerCount());
            pool.close();
        }
    }

    /** {@code Shared} keeps the existing behaviour: concurrent leases mean concurrent producers. */
    @Test
    public void sharedAccessStillCreatesAProducerPerConcurrentLease() throws Exception {
        final PublisherPool pool = poolWith(ProducerAccessMode.Shared);

        final PublisherLease first = pool.obtainPublisher(TOPIC);
        final PublisherLease second = pool.obtainPublisher(TOPIC);

        assertNotSame(first, second);
        verify(builder, times(2)).create();
    }

    /** A configuration that never mentions the access mode is Pulsar's default, {@code Shared}. */
    @Test
    public void anAbsentAccessModeIsShared() throws Exception {
        final PublisherPool pool = new PublisherPool(mock(ComponentLog.class), Collections.emptyMap(), client);

        pool.obtainPublisher(TOPIC);
        pool.obtainPublisher(TOPIC);

        verify(builder, times(2)).create();
    }

    /**
     * Returning the same lease twice must not hand the topic to two tasks: the second close is ignored, so the
     * idle queue holds the lease once and a later pair of callers is still serialised.
     */
    @Test
    public void closingALeaseTwiceReleasesTheTopicOnce() throws Exception {
        final PublisherPool pool = poolWith(ProducerAccessMode.Exclusive);

        final PublisherLease lease = pool.obtainPublisher(TOPIC);
        lease.close();
        lease.close();
        assertEquals(1, pool.getIdleProducerCount());

        final PublisherLease again = pool.obtainPublisher(TOPIC);
        assertSame(lease, again);
        final CompletableFuture<PublisherLease> waiting = CompletableFuture.supplyAsync(() -> pool.obtainPublisher(TOPIC), otherTask);
        Thread.sleep(200);
        assertFalse("a second task got the topic while its only lease was in use", waiting.isDone());

        again.close();
        assertSame(lease, waiting.get(5, TimeUnit.SECONDS));
        verify(builder, times(1)).create();
    }

    /** A producer the client refuses to create must not leave the topic locked for the next caller. */
    @Test
    public void aFailedCreationReleasesTheTopic() throws Exception {
        final PublisherPool pool = poolWith(ProducerAccessMode.Exclusive);
        doAnswer(invocation -> {
            throw new PulsarClientException("broker refused");
        }).when(builder).create();

        assertTrue("obtainPublisher returns null when the producer cannot be created", pool.obtainPublisher(TOPIC) == null);

        final CompletableFuture<PublisherLease> next = CompletableFuture.supplyAsync(() -> pool.obtainPublisher(TOPIC), otherTask);
        // null again (creation still fails), but promptly: the topic was not left locked by the first failure
        assertTrue(next.get(5, TimeUnit.SECONDS) == null);
    }
}
