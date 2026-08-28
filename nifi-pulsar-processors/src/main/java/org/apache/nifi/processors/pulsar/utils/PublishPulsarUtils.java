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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.FlowFileFilters;

import java.util.List;

public class PublishPulsarUtils {

    /** Upper bound on the content pulled into a single trigger. */
    private static final int MAX_BATCH_BYTES = 1;

    /** Upper bound on the number of FlowFiles pulled into a single trigger. */
    private static final int MAX_BATCH_FLOWFILES = 500;

    /**
     * Claims a bounded batch of FlowFiles for one trigger: at most {@value #MAX_BATCH_FLOWFILES} FlowFiles
     * or 1 MB of content, whichever comes first.
     * <p>
     * The size-based filter was already here, but a follow-up loop then drained the rest of the queue in
     * 10,000-FlowFile batches until it was empty, which made the bound meaningless. A large backlog was
     * pulled into a single session: unbounded heap for the batch, and one failure rolling back the entire
     * backlog rather than a bounded slice of it. NiFi calls onTrigger again immediately, so honouring the
     * bound costs no throughput.
     *
     * @param session the current process session
     * @return the FlowFiles claimed for this trigger, empty when the queue is empty
     */
    public static List<FlowFile> pollFlowFiles(final ProcessSession session) {
        return session.get(FlowFileFilters.newSizeBasedFilter(MAX_BATCH_BYTES, DataUnit.MB, MAX_BATCH_FLOWFILES));
    }
}
