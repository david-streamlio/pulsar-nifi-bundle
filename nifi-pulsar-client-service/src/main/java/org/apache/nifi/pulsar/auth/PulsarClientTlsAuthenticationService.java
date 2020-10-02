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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

@Tags({"Pulsar", "client", "security", "authentication", "TLS"})
@CapabilityDescription("Implementation with TLS Authentication of the PulsarClientAuthenticationService. "
        + "Provides Pulsar clients with the ability to authenticate against a "
        + "secured Apache Pulsar broker endpoint.")
public class PulsarClientTlsAuthenticationService extends AbstractPulsarClientAuntenticationService {

    public static final PropertyDescriptor CLIENT_CERTIFICATE = new PropertyDescriptor.Builder()
            .name("Client Certificate")
            .description("The fully-qualified filename of the client certificate.")
            .defaultValue(null)
            .addValidator(createFileExistsAndReadableValidator())
            .sensitive(false)
            .build();

    // client role key
    public static final PropertyDescriptor CLIENT_KEY = new PropertyDescriptor.Builder()
            .name("Client Key")
            .description("The fully-qualified filename of the client private key.")
            .defaultValue(null)
            .addValidator(createFileExistsAndReadableValidator())
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(TRUST_CERTIFICATE);
        props.add(CLIENT_CERTIFICATE);
        props.add(CLIENT_KEY);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Authentication getAuthentication() {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", configContext.getProperty(CLIENT_CERTIFICATE).getValue());
        authParams.put("tlsKeyFile", configContext.getProperty(CLIENT_KEY).getValue());
        try {
          return AuthenticationFactory.create(AuthenticationTls.class.getName(), authParams);
        } catch (UnsupportedAuthenticationException e) {
          getLogger().error("Unable to authenticate", e);
          return null;
        }
    }
}
