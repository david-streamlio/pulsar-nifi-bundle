/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.apache.nifi.testing.*;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.google.protobuf.GeneratedMessageV3;

@SuppressWarnings("rawtypes")
public class ConsumerBuilderFactoryTests {

    private PulsarClient pulsarClient;
	
	@Mock
	private SchemaInfo mockSchema = mock(SchemaInfo.class);
	
	private ConsumerBuilderFactory factory;
	
	@Before
	public final void init() throws PulsarClientException {
	   pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
	   factory = new ConsumerBuilderFactory(pulsarClient);
	}
	
	@Test
	public final void booleanSchemaTest() {
	   when(mockSchema.getType()).thenReturn(SchemaType.BOOLEAN);
	   ConsumerBuilderImpl impl = (ConsumerBuilderImpl)factory.build(mockSchema);
	   assertEquals(Schema.BOOL, impl.getSchema());
	}
	
	@Test
	public final void dateSchemaTest() {
		when(mockSchema.getType()).thenReturn(SchemaType.DATE);
		ConsumerBuilderImpl impl = (ConsumerBuilderImpl)factory.build(mockSchema);
		assertEquals(Schema.DATE, impl.getSchema());
	}
	
	@Test
	public final void avroSchemaTest() {
	   String def = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"org.apache.nifi.processors.pulsar.util.ConsumerBuilderFactoryTests\",\"fields\":[{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
	   
	   AvroSchema<User> userSchema = AvroSchema.of(User.class);
       when(mockSchema.getType()).thenReturn(userSchema.getSchemaInfo().getType());
       when(mockSchema.getSchema()).thenReturn(userSchema.getSchemaInfo().getSchema());
       
       ConsumerBuilderImpl impl = (ConsumerBuilderImpl)factory.build(mockSchema);
       assertEquals(def, impl.getSchema().getSchemaInfo().getSchemaDefinition());
	}
	
	@Test
	public final void jsonSchemaTest() {
	   JSONSchema<User> userSchema = JSONSchema.of(User.class);
	   
	   when(mockSchema.getType()).thenReturn(userSchema.getSchemaInfo().getType());
       when(mockSchema.getSchema()).thenReturn(userSchema.getSchemaInfo().getSchema());
       
       ConsumerBuilderImpl impl = (ConsumerBuilderImpl)factory.build(mockSchema);
       assertEquals("", impl.getSchema().getSchemaInfo().getSchemaDefinition());
	}
	
	@Test
	public final void protobufTest() {
		ProtobufSchema<GeneratedMessageV3> userSchema = 
			ProtobufSchema.of(Person.class, new HashMap<String, String>());
		
		when(mockSchema.getType()).thenReturn(userSchema.getSchemaInfo().getType());
	    when(mockSchema.getSchema()).thenReturn(userSchema.getSchemaInfo().getSchema());
	       
	    ConsumerBuilderImpl impl = (ConsumerBuilderImpl)factory.build(mockSchema);
	    assertEquals("", impl.getSchema().getSchemaInfo().getSchemaDefinition());
	}
	
	private static final class User {
		private String name;
		private int age;
	}
}
