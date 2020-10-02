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
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;


/**
 * https://pulsar.apache.org/docs/en/security-athenz/
 *
 */
@Tags({"Pulsar", "client", "security", "authentication", "Athenz"})
@CapabilityDescription("Implementation with Athenz Authentication of the PulsarClientAuthenticationService. "
        + "Provides Pulsar clients with the ability to authenticate against a "
        + "secured Apache Pulsar broker endpoint.")
public class PulsarClientAthenzAuthenticationService extends AbstractPulsarClientAuntenticationService {

    public static final PropertyDescriptor TENANT_DOMAIN = new PropertyDescriptor.Builder()
            .name("The tenant domain name")
            .description("The domain name for this tenant")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TENANT_SERVICE = new PropertyDescriptor.Builder()
            .name("The tenant service name")
            .description("The service name for this tenant")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor PROVIDER_DOMAIN = new PropertyDescriptor.Builder()
            .name("The provider domain")
            .description("The provider domain name")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TENANT_PRIVATE_KEY_FILE = new PropertyDescriptor.Builder()
            .name("Tenants Private Key Filename")
            .description("The fully-qualified filename of the tenant's private key.")
            .defaultValue(null)
            .addValidator(createFileExistsAndReadableValidator())
            .required(true)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TENANT_PRIVATE_KEY_ID = new PropertyDescriptor.Builder()
            .name("Tenants Private Key Id")
            .description("The id of tenant's private key.")
            .defaultValue("0")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor AUTO_PREFETCH_ENABLED = new PropertyDescriptor.Builder()
            .name("Auto Prefetch Enabled")
            .description("Specifies whether or not ZTS auto prefetching is enabled.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor ATHENZ_CONF_PATH = new PropertyDescriptor.Builder()
            .name("Pulsar Athenz Conf Path")
            .description("The fully-qualified filename of the Pulsar Athenz configuration file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor PRINCIPAL_HEADER = new PropertyDescriptor.Builder()
            .name("Principal Header")
            .description("Header name of Principal Token.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor ROLE_HEADER = new PropertyDescriptor.Builder()
            .name("Role Header")
            .description("Header name of Role Token.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor ZTS_URL = new PropertyDescriptor.Builder()
            .name("ZTS URL")
            .description("The ZTS Server URL.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(TRUST_CERTIFICATE);
        props.add(TENANT_DOMAIN);
        props.add(TENANT_SERVICE);
        props.add(PROVIDER_DOMAIN);
        props.add(TENANT_PRIVATE_KEY_FILE);
        props.add(TENANT_PRIVATE_KEY_ID);
        props.add(AUTO_PREFETCH_ENABLED);
        props.add(ATHENZ_CONF_PATH);
        props.add(PRINCIPAL_HEADER);
        props.add(ROLE_HEADER);
        props.add(ZTS_URL);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Authentication getAuthentication() {
        Map<String, String> authParams = new HashMap<>();

        putAuthParamIfSet(authParams, "tenantDomain", configContext.getProperty(TENANT_DOMAIN));
        putAuthParamIfSet(authParams, "tenantService", configContext.getProperty(TENANT_SERVICE));
        putAuthParamIfSet(authParams, "providerDomain", configContext.getProperty(PROVIDER_DOMAIN));
        putAuthParamIfSet(authParams, "privateKey", configContext.getProperty(TENANT_PRIVATE_KEY_FILE));
        putAuthParamIfSet(authParams, "keyId", configContext.getProperty(TENANT_PRIVATE_KEY_ID));
        putAuthParamIfSet(authParams, "autoPrefetchEnabled", configContext.getProperty(AUTO_PREFETCH_ENABLED));
        putAuthParamIfSet(authParams, "athenzConfPath", configContext.getProperty(ATHENZ_CONF_PATH));
        putAuthParamIfSet(authParams, "principalHeader", configContext.getProperty(PRINCIPAL_HEADER));
        putAuthParamIfSet(authParams, "roleHeader", configContext.getProperty(ROLE_HEADER));
        putAuthParamIfSet(authParams, "ztsUrl", configContext.getProperty(ZTS_URL));

        try {
            return AuthenticationFactory.create(AuthenticationAthenz.class.getName(), authParams);
        } catch (UnsupportedAuthenticationException e) {
            getLogger().error("Unable to authenticate", e);
            return null;
        }
    }

    private void putAuthParamIfSet(Map<String, String> authParams, String key, PropertyValue value) {
        if (value.isSet()) {
            authParams.put(key, value.getValue());
        }
    }

}
