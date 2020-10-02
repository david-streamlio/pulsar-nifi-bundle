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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;

/**
 * http://pulsar.apache.org/docs/en/security-token-client/
 *
 */
@Tags({"Pulsar", "client", "security", "authentication", "JWT"})
@CapabilityDescription("Implementation with JSON Web Token (JWT) Authentication of the PulsarClientAuthenticationService. "
        + "Provides Pulsar clients with the ability to authenticate against a "
        + "secured Apache Pulsar broker endpoint.")
public class PulsarClientJwtAuthenticationService extends AbstractPulsarClientAuntenticationService {

    public static final PropertyDescriptor JWT_TOKEN = new PropertyDescriptor.Builder()
            .name("The JSON Web Token")
            .description("The raw signed JWT string")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(TRUST_CERTIFICATE);
        props.add(JWT_TOKEN);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Authentication getAuthentication() {
       return AuthenticationFactory.token(configContext.getProperty(JWT_TOKEN).getValue());
    }
}
