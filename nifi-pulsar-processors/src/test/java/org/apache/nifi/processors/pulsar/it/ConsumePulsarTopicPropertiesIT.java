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

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.junit.Before;
import org.junit.Test;

/**
 * Read Compacted and the Topics Pattern match mode, against a real broker.
 * <p>
 * Neither can be shown with a mocked client. A compacted read is the broker serving a different view of the
 * topic, and what a pattern matches is decided broker-side. The unit suite covers the validation rules; this
 * covers what the broker then does.
 */
public class ConsumePulsarTopicPropertiesIT extends AbstractPulsarIT {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        addRealPulsarClientService(runner, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "pulsar-client");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_INITIAL_POSITION, "Earliest");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "20");
    }

    /**
     * A Topics Pattern has always defaulted to matching persistent topics only, with no way to say
     * otherwise - so a pattern that plainly matches a non-persistent topic silently did not consume from it.
     * This is the property that fixes that, and the assertion is that the non-persistent topic is now read.
     */
    @Test
    public void aNonPersistentTopicIsMatchedOnlyWhenTheMatchModeAllowsIt() throws Exception {
        final String suffix = "match-" + System.nanoTime();
        final String nonPersistent = "non-persistent://public/default/" + suffix;

        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS_PATTERN,
                "non-persistent://public/default/" + suffix);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "match-sub");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.REGEX_SUBSCRIPTION_MODE, "NonPersistentOnly");
        runner.setProperty(AbstractPulsarConsumerProcessor.PATTERN_AUTO_DISCOVERY_PERIOD, "1 sec");

        // A non-persistent topic drops messages with no connected consumer, so the processor has to be
        // subscribed before anything is published.
        runner.run(1, false, true);

        try (Producer<byte[]> producer = getClient().newProducer(Schema.BYTES).topic(nonPersistent).create()) {
            await("the pattern to discover the non-persistent topic and deliver a message", () -> {
                producer.send("non-persistent-payload".getBytes());
                runner.run(1, false, false);
                return !runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).isEmpty();
            });
        }

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertTrue("nothing was consumed from the non-persistent topic", !flowFiles.isEmpty());
        assertTrue("the consumed content did not come from the non-persistent topic",
                new String(flowFiles.get(0).toByteArray()).contains("non-persistent-payload"));
    }

    /**
     * A compacted read serves the latest value per key rather than the backlog. The topic is written with
     * two values for one key, so a normal read sees both and a compacted read sees only the second.
     * <p>
     * Compaction is triggered explicitly and waited for: without a compacted ledger the broker serves the
     * normal backlog, and the test would pass for the wrong reason on a topic that was simply never
     * compacted.
     */
    @Test
    public void aCompactedReadSeesOnlyTheLatestValuePerKey() throws Exception {
        final String topic = "persistent://public/default/compacted-" + System.nanoTime();

        try (Producer<byte[]> producer = getClient().newProducer(Schema.BYTES).topic(topic).create()) {
            producer.newMessage().key("k1").value("v1-old".getBytes()).send();
            producer.newMessage().key("k1").value("v2-new".getBytes()).send();
        }

        // Compact, and wait for it to actually finish, so there is genuinely a compacted view to read.
        // The admin CLI reports "Compaction was a success" when it has, and "Compaction has not been run
        // for <topic> since broker startup" when it has not - so matching the success string is what keeps
        // this test from passing against an uncompacted topic, where the broker just serves the backlog and
        // the assertions below would be measuring nothing.
        exec("bin/pulsar-admin", "topics", "compact", topic);
        await("compaction to report success", () ->
                exec("bin/pulsar-admin", "topics", "compaction-status", topic).contains("was a success"));

        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, topic);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "compacted-sub");
        // Read Compacted needs a single active consumer; the validator enforces this.
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Exclusive");
        runner.setProperty(AbstractPulsarConsumerProcessor.READ_COMPACTED, "true");
        runner.assertValid();

        await("the compacted view to be delivered", () -> {
            runner.run(1, false, true);
            return !runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).isEmpty();
        });

        final String content = new String(
                runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS).get(0).toByteArray());

        assertTrue("the latest value for the key was not delivered: " + content, content.contains("v2-new"));
        assertTrue("the superseded value was delivered, so this was not a compacted read: " + content,
                !content.contains("v1-old"));
    }

    /** Runs a command inside the broker container and returns its combined output. */
    private static String exec(final String... command) throws Exception {
        final org.testcontainers.containers.Container.ExecResult result = PULSAR.execInContainer(command);
        return result.getStdout() + result.getStderr();
    }
}
