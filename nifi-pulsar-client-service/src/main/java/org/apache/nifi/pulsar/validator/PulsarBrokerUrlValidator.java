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

import org.apache.commons.lang.StringUtils;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

public class PulsarBrokerUrlValidator implements Validator {
	
	public static final String MALFORMED_URL = " isn't a valid URL";
	public static final String UNSUPPORTED_PROTOCOL = " isn't a supported protocol";

	@Override
	public ValidationResult validate(String subject, String input, ValidationContext context) {
		
		boolean valid = true;
		
        String explanation = null, protocol, hostAndPort;
		try {
          protocol = input.substring(0, input.indexOf(":"));
          
          if (isSupportedProtocol(protocol)) {
            hostAndPort = input.substring(input.lastIndexOf("/"), input.length());
            return StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR.validate(subject, hostAndPort, context);
          } else {
        	valid = false;
        	explanation = UNSUPPORTED_PROTOCOL;
          }
          
		} catch (final StringIndexOutOfBoundsException e) {
	      valid = false;
	      explanation = MALFORMED_URL;
		}
		
		return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(valid)
                .explanation(explanation)
                .build();
	}
	
	private static final boolean isSupportedProtocol(String s) {
		return StringUtils.isNotBlank(s) && (s.equalsIgnoreCase("pulsar") || s.equalsIgnoreCase("pulsar+ssl"));
	}

}
