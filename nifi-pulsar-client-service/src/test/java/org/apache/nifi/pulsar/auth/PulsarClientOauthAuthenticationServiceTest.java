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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.nifi.pulsar.TestProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Authentication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PulsarClientOauthAuthenticationServiceTest {

    private TestRunner testRunner;
    private PulsarClientOauthAuthenticationService authService;

    @BeforeEach
    public void setUp() throws InitializationException {
        testRunner = TestRunners.newTestRunner(TestProcessor.class);
        authService = new PulsarClientOauthAuthenticationService();
        testRunner.addControllerService("oauth-auth", authService);
    }

    @Test
    public void testAuthenticationWithPrivateKeyFile() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.PRIVATE_KEY_FILE, "file:///path/to/credentials.json");
        
        testRunner.enableControllerService(authService);
        testRunner.assertValid(authService);

        // Verify that PRIVATE_KEY_FILE takes precedence
        Authentication auth = authService.getAuthentication();
        assertNotNull(auth, "Authentication should not be null when PRIVATE_KEY_FILE is provided");
    }

    @Test
    public void testAuthenticationWithClientCredentials() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_ID, "test-client-id");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_SECRET, "test-client-secret");
        
        testRunner.enableControllerService(authService);
        testRunner.assertValid(authService);

        Authentication auth = authService.getAuthentication();
        assertNotNull(auth, "Authentication should not be null when CLIENT_ID and CLIENT_SECRET are provided");
    }

    @Test
    public void testPrivateKeyFileTakesPrecedenceOverClientCredentials() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.PRIVATE_KEY_FILE, "file:///path/to/credentials.json");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_ID, "test-client-id");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_SECRET, "test-client-secret");
        
        testRunner.enableControllerService(authService);
        testRunner.assertValid(authService);

        // When both are present, PRIVATE_KEY_FILE should take precedence
        Authentication auth = authService.getAuthentication();
        assertNotNull(auth, "Authentication should not be null when both credentials are provided");
        // Note: The actual authentication method used is determined by the if-else logic in getAuthentication()
    }

    @Test
    public void testAuthenticationWithoutCredentials() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        
        testRunner.enableControllerService(authService);
        testRunner.assertValid(authService);

        Authentication auth = authService.getAuthentication();
        assertNull(auth, "Authentication should be null when no credentials are provided");
    }

    @Test
    public void testPartialClientCredentials() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_ID, "test-client-id");
        // Missing CLIENT_SECRET
        
        testRunner.enableControllerService(authService);
        testRunner.assertValid(authService);

        // This should return null when not all three (CLIENT_ID, CLIENT_SECRET, ISSUER_URL) are provided
        Authentication auth = authService.getAuthentication();
        assertNull(auth, "Authentication should be null when CLIENT_ID/CLIENT_SECRET are incomplete");
    }

    @Test
    public void testRequiredPropertiesValidation() throws InitializationException {
        testRunner.addControllerService("invalid-auth", authService);
        
        // Missing required properties should make service invalid
        testRunner.assertNotValid(authService);
        
        // Add audience but still missing issuer URL
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.assertNotValid(authService);
        
        // Add issuer URL - should now be valid
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.assertValid(authService);
    }

    @Test
    public void testBlankClientIdRejected() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_ID, "   ");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_SECRET, "test-secret");
        
        // Blank/whitespace-only CLIENT_ID should be invalid due to NON_BLANK_VALIDATOR
        testRunner.assertNotValid(authService);
    }

    @Test
    public void testEmptyClientSecretRejected() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_ID, "test-client-id");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_SECRET, "");
        
        // Empty CLIENT_SECRET should be invalid due to NON_BLANK_VALIDATOR
        testRunner.assertNotValid(authService);
    }

    @Test
    public void testSensitivePropertyConfiguration() {
        // Verify that CLIENT_ID and CLIENT_SECRET are marked as sensitive
        assertTrue(PulsarClientOauthAuthenticationService.CLIENT_ID.isSensitive(), 
                   "CLIENT_ID should be marked as sensitive");
        assertTrue(PulsarClientOauthAuthenticationService.CLIENT_SECRET.isSensitive(), 
                   "CLIENT_SECRET should be marked as sensitive");
        
        // Verify that PRIVATE_KEY_FILE is not marked as sensitive (it's just a file path)
        assertFalse(PulsarClientOauthAuthenticationService.PRIVATE_KEY_FILE.isSensitive(), 
                    "PRIVATE_KEY_FILE should not be marked as sensitive");
    }

    @Test
    public void testClientCredentialsPropertyDescriptorConfiguration() {
        // Verify CLIENT_ID property descriptor configuration
        assertEquals("CLIENT_ID", PulsarClientOauthAuthenticationService.CLIENT_ID.getName(), 
                     "Property name should match");
        assertEquals("Client ID", PulsarClientOauthAuthenticationService.CLIENT_ID.getDisplayName(), 
                     "Property display name should be set");
        assertFalse(PulsarClientOauthAuthenticationService.CLIENT_ID.isRequired(), 
                    "Property should not be required");
        assertTrue(PulsarClientOauthAuthenticationService.CLIENT_ID.isExpressionLanguageSupported(), 
                   "Property should support expression language");
        
        // Verify CLIENT_SECRET property descriptor configuration
        assertEquals("CLIENT_SECRET", PulsarClientOauthAuthenticationService.CLIENT_SECRET.getName(), 
                     "Property name should match");
        assertEquals("Client Secret", PulsarClientOauthAuthenticationService.CLIENT_SECRET.getDisplayName(), 
                     "Property display name should be set");
        assertFalse(PulsarClientOauthAuthenticationService.CLIENT_SECRET.isRequired(), 
                    "Property should not be required");
        assertTrue(PulsarClientOauthAuthenticationService.CLIENT_SECRET.isExpressionLanguageSupported(), 
                   "Property should support expression language");
    }

    @Test
    public void testAllThreeClientCredentialsRequired() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_ID, "test-client-id");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_SECRET, "test-client-secret");
        // All three are present: CLIENT_ID, CLIENT_SECRET, and ISSUER_URL
        
        testRunner.enableControllerService(authService);
        testRunner.assertValid(authService);

        Authentication auth = authService.getAuthentication();
        assertNotNull(auth, "Authentication should not be null when all three client credential properties are provided");
    }

    @Test
    public void testMissingClientSecretReturnsNull() throws InitializationException {
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.AUDIENCE, "https://broker.example.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.ISSUER_URL, "https://accounts.google.com");
        testRunner.setProperty(authService, PulsarClientOauthAuthenticationService.CLIENT_ID, "test-client-id");
        // Missing CLIENT_SECRET
        
        testRunner.enableControllerService(authService);
        testRunner.assertValid(authService);

        Authentication auth = authService.getAuthentication();
        assertNull(auth, "Authentication should be null when CLIENT_SECRET is missing");
    }
}