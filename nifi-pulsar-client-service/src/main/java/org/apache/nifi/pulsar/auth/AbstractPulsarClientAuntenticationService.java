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

import java.io.File;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.pulsar.client.api.Authentication;

public abstract class AbstractPulsarClientAuntenticationService extends AbstractControllerService
     implements PulsarClientAuthenticationService {

    public static final PropertyDescriptor TRUST_CERTIFICATE = new PropertyDescriptor.Builder()
            .name("Trusted Certificate Filename")
            .description("The fully-qualified filename of the Trusted certificate.")
            .defaultValue(null)
            .addValidator(createFileExistsAndReadableValidator())
            .sensitive(false)
            .build();

    protected ConfigurationContext configContext;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        configContext = context;
    }

    @Override
    public String getTlsTrustCertsFilePath() {
       return configContext.getProperty(TRUST_CERTIFICATE).getValue();
    }

    public abstract Authentication getAuthentication();

    protected static Validator createFileExistsAndReadableValidator() {
        return new Validator() {
            // Not using the FILE_EXISTS_VALIDATOR because the default is to
            // allow expression language
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final File file = new File(input);
                final boolean valid = file.exists() && file.canRead();
                final String explanation = valid ? null : "File " + file + " does not exist or cannot be read";
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(valid)
                        .explanation(explanation)
                        .build();
            }
        };
    }
}
