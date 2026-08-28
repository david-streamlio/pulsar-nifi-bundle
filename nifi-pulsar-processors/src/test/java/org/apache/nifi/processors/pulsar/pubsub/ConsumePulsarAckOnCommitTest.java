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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A message may only be acknowledged once the session carrying its content has committed (issue #167).
 * <p>
 * ConsumePulsar used to acknowledge either before the content was written - on a Shared subscription every
 * message was acknowledged as it was claimed, ahead of the write - or immediately after a rollback:
 * <pre>
 *     session.rollback();
 *     if (!shared) {
 *         consumer.acknowledgeCumulative(lastMsg);
 *     }
 * </pre>
 * Either way the message was gone from Pulsar without ever reaching a FlowFile: the broker does not
 * redeliver an acknowledged message, and nothing routes it to a failure relationship. That is silent,
 * unrecoverable loss on any FlowFile write error.
 */
@RunWith(Parameterized.class)
public class ConsumePulsarAckOnCommitTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/ack-on-commit";
    private static final int MESSAGE_COUNT = 4;

    @Parameters(name = "shared={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    private final boolean shared;

    public ConsumePulsarAckOnCommitTest(final boolean shared) {
        this.shared = shared;
    }

    /**
     * Runs the real processor against a session whose {@code write()} hands back a stream that always
     * throws, which is what a full content repository or a disk error looks like from here.
     */
    public static class FailingWriteConsumePulsar extends ConsumePulsar {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
            super.onTrigger(context, failingWriteSession(session));
        }
    }

    /**
     * A ProcessSession that behaves exactly like the real one except that the OutputStream from
     * {@code write(FlowFile)} refuses to write. The real stream is closed straight away so the session is
     * not left holding an open stream on the FlowFile.
     */
    private static ProcessSession failingWriteSession(final ProcessSession delegate) {
        return (ProcessSession) Proxy.newProxyInstance(
                ConsumePulsarAckOnCommitTest.class.getClassLoader(),
                new Class<?>[] {ProcessSession.class},
                (proxy, method, args) -> {
                    final Object result = method.invoke(delegate, args);

                    if ("write".equals(method.getName()) && result instanceof OutputStream) {
                        ((OutputStream) result).close();
                        return new OutputStream() {
                            @Override
                            public void write(final int b) throws IOException {
                                throw new IOException("Intentional Unit Test Exception: cannot write");
                            }

                            @Override
                            public void write(final byte[] b, final int off, final int len) throws IOException {
                                throw new IOException("Intentional Unit Test Exception: cannot write");
                            }

                            @Override
                            public void close() {
                                // already closed
                            }
                        };
                    }

                    return result;
                });
    }

    private void configure(final Class<? extends ConsumePulsar> processorClass) throws InitializationException {
        runner = TestRunners.newTestRunner(processorClass);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, shared ? "Shared" : "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, String.valueOf(MESSAGE_COUNT));
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        mockClientService.setMockMessageQueue(messages());
    }

    /**
     * The healthy path still acknowledges. Deferring the acknowledgement must not mean dropping it - an
     * unacknowledged message is redelivered forever.
     */
    @Test
    public void messagesAreAcknowledgedOnceTheContentIsCommitted() throws Exception {
        configure(ConsumePulsar.class);

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 1);

        if (shared) {
            verify(mockClientService.getMockConsumer(), times(MESSAGE_COUNT)).acknowledge(any(Message.class));
        } else {
            verify(mockClientService.getMockConsumer(), times(1)).acknowledgeCumulative(any(Message.class));
        }
    }

    /**
     * The bug: when the write fails the session is rolled back, so nothing may be acknowledged. The
     * messages stay with the broker and are redelivered - duplication rather than loss.
     */
    @Test
    public void nothingIsAcknowledgedWhenTheWriteFails() throws Exception {
        configure(FailingWriteConsumePulsar.class);

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsar.REL_SUCCESS, 0);
        verify(mockClientService.getMockConsumer(), never()).acknowledge(any(Message.class));
        verify(mockClientService.getMockConsumer(), never()).acknowledgeCumulative(any(Message.class));
    }

    private static List<Message<GenericRecord>> messages() {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();
        for (int n = 1; n <= MESSAGE_COUNT; n++) {
            msgs.add(new MockPulsarMessage<GenericRecord>(TOPIC, ("message-" + n).getBytes(UTF_8),
                    "1234:" + n + ":0", null, null));
        }
        return msgs;
    }
}
