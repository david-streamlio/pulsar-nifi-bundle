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
package org.apache.nifi.pulsar.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PulsarBrokerUrlValidatorTest {
	
	private PulsarBrokerUrlValidator validator;
	private ValidationResult result;
	
	@Mock
	private ValidationContext ctx;
	private AutoCloseable closeable;
	
	@Before
	public final void init() {
		validator = new PulsarBrokerUrlValidator();
		closeable = MockitoAnnotations.openMocks(this);
	}
	@After
	public final void cleanup() throws Exception {
		closeable.close();
	}

	@Test
	public final void malformedUrlTest() {
		result = validator.validate("PulsarBrokerUrl", "bad", ctx);
		assertFalse(result.isValid());
		assertEquals(PulsarBrokerUrlValidator.MALFORMED_URL, result.getExplanation());
	}
	
	@Test
	public final void invalidProtocolTest() {
		result = validator.validate("PulsarBrokerUrl", "http://localhost:8080", ctx);
		assertFalse(result.isValid());
		assertEquals(PulsarBrokerUrlValidator.UNSUPPORTED_PROTOCOL, result.getExplanation());
	}
	
	@Test
	public final void invalidHostPort() {
		result = validator.validate("PulsarBrokerUrl", "pulsar://foobar", ctx);
		assertFalse(result.isValid());
		assertEquals("Must be in hostname:port form (no scheme such as http://", result.getExplanation());
	}
	
	@Test
	public final void validUrlTest() {
		result = validator.validate("PulsarBrokerUrl", "pulsar://host.testing.com:6650", ctx);
		assertTrue(result.isValid());
	}
	
	@Test
	public final void validSSLTest() {
		result = validator.validate("PulsarBrokerUrl", "pulsar+ssl://host.testing.com:6651", ctx);
		assertTrue(result.isValid());
	}
}
