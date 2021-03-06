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

import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.After;

public abstract class AbstractPulsarProcessorTest<T> {

    protected TestRunner runner;

    protected MockPulsarClientService<T> mockClientService;

    protected boolean isSharedSubType(String subType) {
    	return subType.equalsIgnoreCase("Shared") || subType.equalsIgnoreCase("Key_Shared");
    }
    
    protected void addPulsarClientService() throws InitializationException {
        mockClientService = new MockPulsarClientService<T>();
        runner.addControllerService("Pulsar Client Service", mockClientService);
        runner.enableControllerService(mockClientService);
        runner.setProperty(AbstractPulsarConsumerProcessor.PULSAR_CLIENT_SERVICE, "Pulsar Client Service");
    }

    @After
    public final void validate() {
        org.mockito.Mockito.validateMockitoUsage();
    }
}
