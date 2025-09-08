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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.utils.PublishPulsarUtils;
import org.apache.nifi.processors.pulsar.utils.PublisherLease;

@SeeAlso({ConsumePulsar.class, ConsumePulsarRecord.class, PublishPulsarRecord.class})
@Tags({"Apache", "Pulsar", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Pulsar using the Pulsar Producer API."
    + "The messages to send may be individual FlowFiles or may be delimited, using a user-specified delimiter, such as "
    + "a new-line. The complementary NiFi processor for fetching messages is ConsumePulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "This attribute is added only to FlowFiles that are routed to success.")
@TriggerWhenEmpty
@SupportsBatching
public class PublishPulsar extends AbstractPulsarProducerProcessor<byte[]> {

    // Current lease to reuse for consecutive FlowFiles with the same topic
    private volatile PublisherLease currentLease = null;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final List<FlowFile> flowFiles = PublishPulsarUtils.pollFlowFiles(session);

        if (flowFiles.isEmpty()) {
            // Because we TriggerWhenEmpty, the framework can give us many more threads that we actually need,
            // so yield when there is no work to do.
            context.yield();
            return;
        }

        final Iterator<FlowFile> itr = flowFiles.iterator();

        while (itr.hasNext()) {
            final FlowFile flowFile = itr.next();
            final String topicName = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
            final boolean asyncFlag = (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean());

            PublisherLease lease = obtainPublisherLease(topicName);

            if (lease == null) {
                getLogger().error("Unable to publish to topic {}", new Object[] {topicName});
                session.transfer(flowFile, REL_FAILURE);
            } else {

                InputStream in = session.read(flowFile);
                try {

                    lease.publish(flowFile, in,
                            getMessageKey(context, flowFile),
                            getMappedMessageProperties(context, flowFile),
                            getDemarcatorBytes(context, flowFile), asyncFlag);

                    IOUtils.closeQuietly(in);
                    session.transfer(flowFile, REL_SUCCESS);
                } catch (Exception ex) {
                    getLogger().error("Unable to process session due to ", ex);
                    IOUtils.closeQuietly(in);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        }
    }

    /**
     * Obtain a PublisherLease, reusing the current lease if it's for the same topic,
     * otherwise closing the existing lease and obtaining a new one.
     * 
     * @param topicName the topic name for the desired lease
     * @return a PublisherLease for the specified topic, or null if unable to obtain one
     */
    private synchronized PublisherLease obtainPublisherLease(final String topicName) {
        // Check if we can reuse the existing lease
        if (currentLease != null) {
            final String currentTopicName = currentLease.getTopicName();
            if (currentTopicName != null && topicName != null && topicName.equals(currentTopicName)) {
                getLogger().debug("Reusing existing publisher lease for topic: {}", topicName);
                return currentLease;
            }
        }
        
        // Close the existing lease if it's for a different topic or if topic names are null
        if (currentLease != null) {
            final String currentTopicName = currentLease.getTopicName();
            getLogger().debug("Closing publisher lease for topic: {} to create new lease for topic: {}", 
                             currentTopicName != null ? currentTopicName : "null", topicName);
            currentLease.close();
            currentLease = null;
        }
        
        // Obtain a new lease for the requested topic
        currentLease = getPublisherPool().obtainPublisher(topicName);
        if (currentLease != null) {
            getLogger().debug("Created new publisher lease for topic: {}", topicName);
        } else {
            getLogger().error("Failed to create publisher lease for topic: {}", topicName);
        }
        
        return currentLease;
    }

    /**
     * Clean up resources when the processor is unscheduled
     */
    @OnUnscheduled
    public synchronized void onUnscheduled() {
        if (currentLease != null) {
            final String currentTopicName = currentLease.getTopicName();
            getLogger().debug("Closing publisher lease during processor unscheduling for topic: {}", 
                             currentTopicName != null ? currentTopicName : "null");
            currentLease.close();
            currentLease = null;
        }
    }
}
