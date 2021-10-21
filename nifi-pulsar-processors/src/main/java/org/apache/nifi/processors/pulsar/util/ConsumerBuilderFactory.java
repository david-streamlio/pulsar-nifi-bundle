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

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Creates a schema-aware consumer based on the SchemaInfo
 * provided. This allows us to consume from existing Pulsar
 * topics that have an associated schema.
 * 
 * Currently, it only supports Primitive schema types.
 */
public class ConsumerBuilderFactory {
	
	private PulsarClient client;
	
	public ConsumerBuilderFactory(PulsarClient c) {
		this.client = c;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ConsumerBuilder build(SchemaInfo info) {
		
		switch (info.getType()) {
		
		  case AVRO :
			  return client.newConsumer(
					AvroSchema.of(
					  SchemaDefinition.builder()
					    .withJsonDef(new String(info.getSchema()))
					    .build())
					);
		
		  case JSON : 
			  return client.newConsumer(
				  JSONSchema.of(
					SchemaDefinition.builder()
					   .withJsonDef(new String(info.getSchema()))
					   .build())
				  );
		  
		  case PROTOBUF : 
			  return client.newConsumer(
				  ProtobufSchema.of(
						SchemaDefinition.builder()
						   .withJsonDef(new String(info.getSchema()))
						   .build())
				  );
		  
		  case BOOLEAN : return client.newConsumer(Schema.BOOL);
		  
		  case DATE : return client.newConsumer(Schema.DATE);
		  
		  case DOUBLE : return client.newConsumer(Schema.DOUBLE);
		  
		  case FLOAT : return client.newConsumer(Schema.FLOAT);
		  
		  case INSTANT : return client.newConsumer(Schema.INSTANT);
		  
		  case INT8 : return client.newConsumer(Schema.INT8);
		  
		  case INT16 : return client.newConsumer(Schema.INT16);
		  
		  case INT32 : return client.newConsumer(Schema.INT32);
		  
		  case INT64 : return client.newConsumer(Schema.INT64);
		  
		  case LOCAL_DATE : return client.newConsumer(Schema.LOCAL_DATE);
		  
		  case LOCAL_DATE_TIME : return client.newConsumer(Schema.LOCAL_DATE_TIME);
		  
		  case LOCAL_TIME : return client.newConsumer(Schema.LOCAL_TIME);
		  
		  case STRING : return client.newConsumer(Schema.STRING);
		  
		  case TIME : return client.newConsumer(Schema.TIME);
		  
		  case TIMESTAMP : return client.newConsumer(Schema.TIMESTAMP);
		  
		  case BYTES :
		  default: return client.newConsumer(Schema.BYTES);
			
		}
		
	}
}
