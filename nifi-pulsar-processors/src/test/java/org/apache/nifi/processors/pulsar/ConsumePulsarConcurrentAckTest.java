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
package org.apache.nifi.processors.pulsar;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * Concurrent tasks share one consumer, so a cumulative acknowledgement from one task acknowledges
 * messages another task is still holding.
 * <p>
 * {@code getConsumer()} is synchronized and caches by consumer id, so every concurrent task of one
 * processor receives from the same {@link Consumer}. That is deliberate - it is what stops the broker
 * refusing a second consumer on an Exclusive subscription. The consequence is that
 * {@code acknowledgeCumulative(last)} is no longer confined to one task's batch: cumulative means "this
 * message and everything before it on the subscription", and everything before it includes whatever the
 * other task is still working on.
 * <p>
 * The losing interleaving: task B commits its batch and cumulatively acknowledges, which acknowledges
 * task A's older message too; task A then fails to write and negatively acknowledges, which does nothing
 * because the message is already acknowledged. The message is neither in NiFi nor recoverable from
 * Pulsar - the same loss the commit-then-acknowledge ordering was introduced to eliminate, reached by a
 * different route.
 *
 * @see <a href="https://github.com/david-streamlio/pulsar-nifi-bundle/issues/223">#223</a>
 */
public class ConsumePulsarConcurrentAckTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/concurrent-ack";

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
    }

    /**
     * The defect, on the subscription types that use cumulative acknowledgement. Task B's batch is
     * committed while task A's older message is still outstanding; nothing may acknowledge task A's
     * message on task B's behalf.
     */
    @Test
    public void oneTasksCommitDoesNotAcknowledgeAnothersOutstandingMessages() throws PulsarClientException {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.run(1, false, true);

        final Consumer<GenericRecord> consumer = mockClientService.getMockConsumer();
        final Message<GenericRecord> taskAMessage = message(1);
        final Message<GenericRecord> taskBMessage = message(2);

        // Task A is holding its message, uncommitted, when task B commits and acknowledges its own.
        final List<Message<GenericRecord>> taskABatch = new ArrayList<>(List.of(taskAMessage));
        final List<Message<GenericRecord>> taskBBatch = new ArrayList<>(List.of(taskBMessage));

        final AbstractPulsarConsumerProcessor<?> processor =
                (AbstractPulsarConsumerProcessor<?>) runner.getProcessor();
        final MockProcessSession taskBSession = (MockProcessSession) runner.getProcessSessionFactory().createSession();

        processor.commitAndAcknowledge(taskBSession, consumer, taskBBatch, false);
        taskBSession.commitAsync();

        // Cumulative acknowledgement means "this message and everything before it on the subscription",
        // so acknowledging task B's message here also acknowledges task A's, which task A still holds and
        // may yet fail to write. Nothing about task B's batch may reach task A's message.
        verify(consumer, never()).acknowledgeCumulative(any(Message.class));
        verify(consumer, never()).acknowledgeCumulativeAsync(any(Message.class));

        // Task A's message must not have been acknowledged at all - it is still in flight.
        verify(consumer, never()).acknowledge(taskAMessage);
    }

    /** Task B's own message is still acknowledged: the fix must not stop acknowledging what it should. */
    @Test
    public void aCommittedBatchIsStillAcknowledged() throws PulsarClientException {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.run(1, false, true);

        final Consumer<GenericRecord> consumer = mockClientService.getMockConsumer();
        final Message<GenericRecord> taskBMessage = message(2);
        final List<Message<GenericRecord>> batch = new ArrayList<>(List.of(taskBMessage));

        final AbstractPulsarConsumerProcessor<?> processor =
                (AbstractPulsarConsumerProcessor<?>) runner.getProcessor();
        final MockProcessSession session = (MockProcessSession) runner.getProcessSessionFactory().createSession();

        processor.commitAndAcknowledge(session, consumer, batch, false);
        session.commitAsync();

        verify(consumer).acknowledge(taskBMessage);
    }

    /** Shared subscriptions already acknowledged per message; that must be unchanged. */
    @Test
    public void aSharedSubscriptionStillAcknowledgesPerMessage() throws PulsarClientException {
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.run(1, false, true);

        final Consumer<GenericRecord> consumer = mockClientService.getMockConsumer();
        final Message<GenericRecord> first = message(1);
        final Message<GenericRecord> second = message(2);
        final List<Message<GenericRecord>> batch = new ArrayList<>(List.of(first, second));

        final AbstractPulsarConsumerProcessor<?> processor =
                (AbstractPulsarConsumerProcessor<?>) runner.getProcessor();
        final MockProcessSession session = (MockProcessSession) runner.getProcessSessionFactory().createSession();

        processor.commitAndAcknowledge(session, consumer, batch, false);
        session.commitAsync();

        verify(consumer).acknowledge(first);
        verify(consumer).acknowledge(second);
        verify(consumer, never()).acknowledgeCumulative(any(Message.class));
    }

    private static Message<GenericRecord> message(final int n) {
        return new MockPulsarMessage<GenericRecord>(TOPIC, ("payload-" + n).getBytes(UTF_8), "1234:" + n + ":0", null, null);
    }
}
