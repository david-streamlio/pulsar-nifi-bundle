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
import static org.junit.Assert.assertTrue;

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * A trigger must claim a bounded batch of FlowFiles, not the whole queue.
 * <p>
 * {@code pollFlowFiles()} started with a size-based filter of 1 MB or 500 FlowFiles and then ran a second
 * loop that drained the rest of the queue in 10,000-FlowFile batches until it was empty, which made the
 * bound meaningless. A large backlog was pulled into one session: unbounded heap for the batch, and one
 * failure rolling back the whole backlog instead of a bounded slice.
 */
public class PublishPulsarBatchBoundTest extends AbstractPulsarProcessorTest<byte[]> {

    /** The FlowFile ceiling in PublishPulsarUtils. */
    private static final int BATCH_LIMIT = 500;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        addPulsarClientService();
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "test-topic");
    }

    /** With a backlog far larger than the limit, one trigger must take a batch and leave the rest queued. */
    @Test
    public void oneTriggerClaimsAtMostOneBatch() {
        for (int n = 0; n < 1_200; n++) {
            runner.enqueue(("message-" + n).getBytes(UTF_8));
        }

        runner.run(1);

        final int published = runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).size();
        assertEquals("a single trigger should claim exactly one batch of " + BATCH_LIMIT, BATCH_LIMIT, published);
        assertEquals("the rest of the backlog should still be queued", 1_200 - BATCH_LIMIT, runner.getQueueSize().getObjectCount());
    }

    /** The backlog is still fully drained, just across triggers rather than in one session. */
    @Test
    public void repeatedTriggersDrainTheBacklog() {
        for (int n = 0; n < 1_200; n++) {
            runner.enqueue(("message-" + n).getBytes(UTF_8));
        }

        runner.run(3);

        runner.assertQueueEmpty();
        assertEquals(1_200, runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).size());
    }

    /** A backlog under the limit is taken in one go, so small flows are unaffected. */
    @Test
    public void aSmallBacklogIsTakenInASingleTrigger() {
        for (int n = 0; n < 20; n++) {
            runner.enqueue(("message-" + n).getBytes(UTF_8));
        }

        runner.run(1);

        runner.assertQueueEmpty();
        assertEquals(20, runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).size());
    }

    /** The byte ceiling applies too: large FlowFiles stop the batch well before the count limit. */
    @Test
    public void theByteCeilingBoundsTheBatchBeforeTheCountDoes() {
        final byte[] chunk = new byte[128 * 1024];   // 128 KB each, so ~8 fill the 1 MB ceiling
        for (int n = 0; n < 100; n++) {
            runner.enqueue(chunk);
        }

        runner.run(1);

        final int published = runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).size();
        assertTrue("1 MB of content should bound the batch well below the " + BATCH_LIMIT
                + " FlowFile limit, but took " + published, published < 100);
        assertTrue("the batch should still make progress, but took " + published, published > 0);
    }
}
