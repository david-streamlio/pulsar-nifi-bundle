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
package org.apache.nifi.processors.pulsar.pubsub.mocks;

import java.io.InputStream;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

/**
 * A {@link RecordReaderFactory} that never manages to resolve a schema. It makes
 * {@code ConsumePulsarRecord#getSchema()} return null for every message, which sends each message to
 * the parse-failure path without ever opening a record set.
 */
public class MockFailingRecordParser extends AbstractControllerService implements RecordReaderFactory {

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength,
                                           ComponentLog logger) throws SchemaNotFoundException {
        throw new SchemaNotFoundException("Intentional Unit Test Exception: no schema could be resolved");
    }
}
