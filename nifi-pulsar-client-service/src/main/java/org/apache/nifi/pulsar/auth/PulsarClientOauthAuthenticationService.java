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
package org.apache.nifi.pulsar.auth;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

@Tags({"Pulsar", "client", "security", "authentication", "Oauth"})
@CapabilityDescription("Implementation with Oauth Authentication of the PulsarClientAuthenticationService. "
        + "Provides Pulsar clients with the ability to authenticate against a "
        + "secured Apache Pulsar broker endpoint.")
public class PulsarClientOauthAuthenticationService extends AbstractPulsarClientAuntenticationService {

	public static final PropertyDescriptor AUDIENCE = new PropertyDescriptor.Builder()
            .name("AUDIENCE")
            .description("An OAuth 2.0 \"resource server\" identifier for the Pulsar cluster, e.g., https://broker.example.com")
            .defaultValue(null)
            .displayName("Audience")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor ISSUER_URL = new PropertyDescriptor.Builder()
			.name("ISSUER_URL")
			.defaultValue(null)
			.description("URL of the authentication provider which allows the Pulsar client to obtain an access token, e.g.,"
					+ "https://accounts.google.com")
			.displayName("Issuer URL")
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor PRIVATE_KEY_FILE = new PropertyDescriptor.Builder()
            .name("PRIVATE_KEY_FILE")
            .description("URL to a JSON credentials file, e.g., file:///path/to/file")
            .defaultValue(null)
            .displayName("Private key file")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(AUDIENCE);
        props.add(ISSUER_URL);
        props.add(PRIVATE_KEY_FILE);
        props.add(TRUST_CERTIFICATE);
        properties = Collections.unmodifiableList(props);
    }
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    
	@Override
	public Authentication getAuthentication() {
		try {
			return AuthenticationFactoryOAuth2.clientCredentials(
					new URL(configContext.getProperty(ISSUER_URL).evaluateAttributeExpressions().getValue()), 
					new URL(configContext.getProperty(PRIVATE_KEY_FILE).evaluateAttributeExpressions().getValue()), 
					configContext.getProperty(AUDIENCE).evaluateAttributeExpressions().getValue());
		} catch (Exception e) {
			getLogger().error("Unable to authenticate", e);
			return null;
		}
	}

}
