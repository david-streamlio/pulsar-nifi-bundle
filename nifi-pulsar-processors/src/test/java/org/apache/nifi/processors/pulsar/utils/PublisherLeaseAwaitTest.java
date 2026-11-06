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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;

import org.junit.jupiter.api.Test;

/**
 * Regression test for the batching path of {@link PublisherLease}.
 * <p>
 * Every 100 sends the lease used to call {@code futures.stream().map(future -> future.get())} with no
 * terminal operation. Streams are lazy, so nothing was ever waited on, and the list was cleared anyway:
 * for any FlowFile larger than one batch the sends became fire and forget. The existing
 * {@code testBulkRecordSuccess} only caught this intermittently (999 sends instead of 1000, depending on
 * timing), so it read as a flaky test rather than a real defect.
 * <p>
 * This test makes it deterministic: each send completes only when the test releases it, so a lease that
 * does not wait provably returns early.
 */
public class PublisherLeaseAwaitTest {

    /** Comfortably more than one 100-message batch, so the batching branch is exercised several times. */
    private static final int MESSAGE_COUNT = 350;

    /**
     * A lease whose sends complete only when {@link #releaseAll()} is called, so "did publish() wait?"
     * becomes a question about state rather than about timing.
     */
    private static final class ControllablePublisherLease extends PublisherLease {

        private final java.util.List<CompletableFuture<MessageId>> issued =
                Collections.synchronizedList(new java.util.ArrayList<>());

        ControllablePublisherLease(final Producer producer, final ComponentLog logger) {
            super(producer, logger);
        }

        @Override
        protected CompletableFuture<MessageId> send(final Producer producer, final String key,
                                                    final java.util.Map<String, String> properties,
                                                    final byte[] value) {
            final CompletableFuture<MessageId> future = new CompletableFuture<>();
            issued.add(future);
            return future;
        }

        /** Completes everything issued so far, on another thread, mimicking broker acknowledgements. */
        void releaseAll() {
            new Thread(() -> {
                while (true) {
                    final java.util.List<CompletableFuture<MessageId>> snapshot;
                    synchronized (issued) {
                        snapshot = new java.util.ArrayList<>(issued);
                    }
                    snapshot.forEach(f -> f.complete(mock(MessageId.class)));
                    if (snapshot.size() == MESSAGE_COUNT) {
                        return;
                    }
                    try {
                        Thread.sleep(5);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "release-sends").start();
        }

        int issuedCount() {
            return issued.size();
        }

        /**
         * Counts the issued sends that are done, which is exactly what {@code publish()} waits on.
         * <p>
         * This used to count with {@code future.thenRun(completed::incrementAndGet)}, which raced: completing
         * a future wakes threads blocked in {@code get()} and runs dependent actions in the same pass, and
         * the publisher's waiter, registered last, is released first. {@code publish()} could return and the
         * test read the counter before the last increment ran, failing with 349 of 350 in CI.
         */
        int completedCount() {
            synchronized (issued) {
                return (int) issued.stream().filter(CompletableFuture::isDone).count();
            }
        }
    }

    private static InputStream body() {
        return new ByteArrayInputStream(IntStream.rangeClosed(1, MESSAGE_COUNT)
                .mapToObj(n -> "message-" + n)
                .collect(Collectors.joining("\n")).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Deterministic discriminator for the dead-stream bug, without relying on timing luck.
     * <p>
     * A lease that waits at each batch boundary cannot issue send number 102 until the first 100 have
     * completed, so with nothing completed it parks after ~100 sends. The unfixed lease waited only on the
     * final partial batch, so it raced through all {@value #MESSAGE_COUNT} sends without ever blocking.
     * Asserting on how far it got while nothing is completed separates the two exactly.
     */
    @Test
    public void publishBlocksAtEachBatchBoundary() throws Exception {
        final Producer<?> producer = mock(Producer.class);
        final ControllablePublisherLease lease =
                new ControllablePublisherLease(producer, new MockComponentLog("id", new Object()));

        final Thread publisher = new Thread(() -> {
            try {
                lease.publish(new MockFlowFile(1L), body(), null, Collections.emptyMap(),
                        "\n".getBytes(StandardCharsets.UTF_8), false);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }, "publisher");
        publisher.setDaemon(true);
        publisher.start();

        // nothing is completed, so a correct lease is parked waiting for its first batch
        Thread.sleep(1500);
        final int issuedWhileBlocked = lease.issuedCount();
        assertTrue(issuedWhileBlocked < MESSAGE_COUNT,
                "publish() issued " + issuedWhileBlocked + " of " + MESSAGE_COUNT + " sends without ever "
                + "waiting for a batch to be confirmed - the batch boundary is not awaited");

        lease.releaseAll();
        publisher.join(30_000);
        assertFalse(publisher.isAlive(), "publish() did not return after all sends were confirmed");
        assertEquals(MESSAGE_COUNT, lease.issuedCount(), "every message should have been sent");
        assertEquals(MESSAGE_COUNT,
                lease.completedCount(), "publish() must not return until every send has completed");
    }

    /** complete() must report what the broker confirmed, not how many records were read off the input. */
    @Test
    public void messageCountReflectsConfirmedSends() throws IOException {
        final Producer<?> producer = mock(Producer.class);
        final ControllablePublisherLease lease =
                new ControllablePublisherLease(producer, new MockComponentLog("id", new Object()));

        final FlowFile flowFile = new MockFlowFile(1L);
        final String body = IntStream.rangeClosed(1, MESSAGE_COUNT)
                .mapToObj(n -> "message-" + n)
                .collect(Collectors.joining("\n"));
        final InputStream in = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));

        lease.releaseAll();
        lease.publish(flowFile, in, null, Collections.emptyMap(), "\n".getBytes(StandardCharsets.UTF_8), false);

        assertEquals(MESSAGE_COUNT, lease.complete());
    }
}
