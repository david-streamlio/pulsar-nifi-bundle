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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Coverage for issue #18: "When you stop and restart a consumer, some messages are skipped."
 * <p>
 * The report describes a processor that, once stopped and restarted, never catches up with its producer.
 * It was never actionable because it cannot be reproduced against a mocked client: what happens to
 * unacknowledged messages when a consumer closes is entirely broker behaviour. With a real broker it
 * becomes a plain question - publish a known set, consume part of it, restart, and check whether the union
 * of both runs is the whole set.
 * <p>
 * These pass on current main, and also on 2046d3d, the commit before any of this year's fixes, so the
 * reported behaviour does not reproduce here rather than having been fixed along the way. They are kept as
 * regression coverage for the acknowledgement and restart path, which nothing else exercises.
 * <p>
 * Duplicates across a restart are acceptable: Pulsar redelivers anything unacknowledged, and NiFi
 * processors are at-least-once. Losing a message is not.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarRestartIT extends AbstractPulsarIT {

    private static final int MESSAGE_COUNT = 60;

    /** Consecutive quiet triggers before a run is considered drained. */
    private static final int IDLE_TRIGGERS_BEFORE_DONE = 15;

    /**
     * Pause after a trigger that produced nothing. The processor polls with {@code receive(0, SECONDS)},
     * which returns immediately, so without this the idle triggers all elapse in a few milliseconds and a
     * run is declared drained before the broker has delivered anything at all.
     */
    private static final long QUIET_TRIGGER_PAUSE_MILLIS = 200L;

    /** Hard stop so a genuinely stuck consumer fails the test rather than spinning. */
    private static final int MAX_TRIGGERS = 400;

    @Parameters(name = "async={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    private final boolean async;

    public ConsumePulsarRestartIT(final boolean async) {
        this.async = async;
    }

    /** Consume part of a topic, stop, restart on the same subscription, and account for every message. */
    @Test
    public void noMessagesAreSkippedAcrossARestart() throws Exception {
        final String topic = "persistent://public/default/restart-" + System.nanoTime();
        final String subscription = "restart-sub";

        // Subscribe before publishing so the durable subscription exists from message one, which is what a
        // running processor would have done.
        getClient().newConsumer().topic(topic).subscriptionName(subscription).subscribe().close();

        publish(topic, IntStream.rangeClosed(1, MESSAGE_COUNT)
                .mapToObj(n -> "message-" + n).toArray(String[]::new));

        final Set<String> consumed = new LinkedHashSet<>(consume(topic, subscription, MESSAGE_COUNT / 3));
        assertTrue("the first run should have consumed something to restart from, but got nothing",
                !consumed.isEmpty());

        // Restart: a brand new processor on the same durable subscription, drained this time.
        consumed.addAll(consume(topic, subscription, Integer.MAX_VALUE));

        assertNothingMissing(consumed, "across the restart");
    }

    /**
     * Repeated stop/start cycles must not lose anything either - a consumer that never catches up is what
     * a small loss per restart would look like over time.
     */
    @Test
    public void noMessagesAreSkippedAcrossRepeatedRestarts() throws Exception {
        final String topic = "persistent://public/default/restart-many-" + System.nanoTime();
        final String subscription = "restart-many-sub";

        getClient().newConsumer().topic(topic).subscriptionName(subscription).subscribe().close();
        publish(topic, IntStream.rangeClosed(1, MESSAGE_COUNT)
                .mapToObj(n -> "message-" + n).toArray(String[]::new));

        final Set<String> consumed = new LinkedHashSet<>();
        for (int cycle = 0; cycle < 6; cycle++) {
            consumed.addAll(consume(topic, subscription, 8));
        }
        // a final drained pass to pick up anything still outstanding
        consumed.addAll(consume(topic, subscription, Integer.MAX_VALUE));

        assertNothingMissing(consumed, "across six restarts");
    }

    /**
     * The reported scenario exactly: the producer keeps going while the consumer is stopped.
     * <p>
     * The other tests publish everything before consuming, so the whole backlog is already behind the
     * subscription cursor. The report describes a producer running continuously - "let it run for a bit,
     * stop the processor with the UI, wait 30 seconds then restart it" - which is the case where messages
     * arrive with no consumer connected at all. If a durable subscription were losing its place, this is
     * where it would show.
     */
    @Test
    public void messagesPublishedWhileStoppedAreDeliveredOnRestart() throws Exception {
        final String topic = "persistent://public/default/restart-downtime-" + System.nanoTime();
        final String subscription = "restart-downtime-sub";

        getClient().newConsumer().topic(topic).subscriptionName(subscription).subscribe().close();

        // a first wave, consumed by a running processor
        publish(topic, IntStream.rangeClosed(1, 20).mapToObj(n -> "message-" + n).toArray(String[]::new));
        final Set<String> consumed = new LinkedHashSet<>(consume(topic, subscription, Integer.MAX_VALUE));

        // the processor is stopped now, and the producer keeps going
        publish(topic, IntStream.rangeClosed(21, 40).mapToObj(n -> "message-" + n).toArray(String[]::new));
        Thread.sleep(2_000L);
        publish(topic, IntStream.rangeClosed(41, MESSAGE_COUNT)
                .mapToObj(n -> "message-" + n).toArray(String[]::new));

        consumed.addAll(consume(topic, subscription, Integer.MAX_VALUE));

        assertNothingMissing(consumed, "while the processor was stopped");
    }

    // ------------------------------------------------------------------ helpers

    private static void assertNothingMissing(final Set<String> consumed, final String when) {
        final List<String> missing = IntStream.rangeClosed(1, MESSAGE_COUNT)
                .mapToObj(n -> "message-" + n)
                .filter(m -> !consumed.contains(m))
                .collect(Collectors.toList());

        assertTrue("Messages were skipped " + when + ": " + missing.size() + " of " + MESSAGE_COUNT
                + " never arrived - " + missing, missing.isEmpty());
    }

    /**
     * Runs a fresh processor against the subscription until it stops producing FlowFiles - or until it has
     * emitted {@code stopAfter} messages - then stops it the way NiFi does, and returns what it emitted.
     * <p>
     * Draining until idle rather than running a fixed number of triggers matters: the processor polls with
     * {@code receive(0, SECONDS)}, which is non-blocking, so the first triggers after a consumer connects
     * routinely return nothing while the broker is still delivering into the receiver queue. Counting
     * triggers measures that startup latency rather than whether messages were skipped.
     *
     * @param topic the topic to consume
     * @param subscription the durable subscription to resume
     * @param stopAfter stop once this many messages have been emitted; {@link Integer#MAX_VALUE} to drain
     * @return every message body the processor emitted
     */
    private List<String> consume(final String topic, final String subscription, final int stopAfter)
            throws InitializationException, InterruptedException {

        final TestRunner runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, subscription);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "5");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");

        int idleTriggers = 0;
        for (int trigger = 0; trigger < MAX_TRIGGERS && idleTriggers < IDLE_TRIGGERS_BEFORE_DONE; trigger++) {
            final int before = bodies(runner).size();
            runner.run(1, false);

            if (bodies(runner).size() == before) {
                idleTriggers++;
                Thread.sleep(QUIET_TRIGGER_PAUSE_MILLIS);
            } else {
                idleTriggers = 0;
            }

            if (bodies(runner).size() >= stopAfter) {
                break;
            }
        }

        // stopOnFinish runs @OnUnscheduled and @OnStopped: the stop half of the restart being tested
        runner.run(1, true);

        return bodies(runner);
    }

    /** Every message body emitted so far, split back out of the demarcated FlowFiles. */
    private static List<String> bodies(final TestRunner runner) {
        final List<String> bodies = new ArrayList<>();
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS)) {
            for (final String line : new String(flowFile.toByteArray(), UTF_8).split("\n")) {
                if (!line.isEmpty()) {
                    bodies.add(line);
                }
            }
        }
        return bodies;
    }
}
