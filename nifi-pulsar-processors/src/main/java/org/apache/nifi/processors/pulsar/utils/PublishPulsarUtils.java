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

import java.util.ArrayList;
import java.util.List;

public class PublishPulsarUtils {

    public static List<FlowFile> pollFlowFiles(final ProcessSession session) {
        final List<FlowFile> initialFlowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 500));

        if (initialFlowFiles.isEmpty()) {
            return initialFlowFiles;
        } else {
            return pollAllFlowFiles(session, initialFlowFiles);
        }

    }

    private static List<FlowFile> pollAllFlowFiles(ProcessSession session, List<FlowFile> initialFlowFiles) {
        final List<FlowFile> polled = new ArrayList<>(initialFlowFiles);
        while (true) {
            final List<FlowFile> flowFiles = session.get(10_000);
            if (flowFiles.isEmpty()) {
                break;
            }

            polled.addAll(flowFiles);
        }

        return polled;
    }
}
