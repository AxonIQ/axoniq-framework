/*
 * Copyright (c) 2010-2025. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.framework.dataprotection.internal.licensing;

import io.axoniq.framework.dataprotection.api.ConfigurationException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SystemStubsExtension.class)
public class LicenseConfigurationTest {

    private static final String PATH_TO_VALID_LICENSE = "axoniq.license";
    private static final String PATH_TO_INVALID_LICENSE = "axoniq.invalid.license";
    private Instant expiryDate;
    private Instant graceDate;

    @SystemStub
    private EnvironmentVariables environmentVariables;

    @BeforeEach
    public void setUp() throws Exception {
        LicenseConfiguration.reset();

        try (InputStream resourceAsStream = new FileInputStream(PATH_TO_VALID_LICENSE)) {
            Properties properties = new Properties();
            properties.load(resourceAsStream);
            this.expiryDate = Instant.parse(properties.getProperty("expiry_date") + "T00:00:00Z");
            this.graceDate = Instant.parse(properties.getProperty("grace_date") + "T00:00:00Z");
        }
    }

    @AfterEach
    public void tearDown() {
        LicenseConfiguration.clock = Clock.systemDefaultZone();
        System.clearProperty("axoniq.dataprotection.license");
        if (environmentVariables != null) {
            environmentVariables.remove("AXONIQ_DATAPROTECTION_LICENSE");
            environmentVariables.remove("AXONIQ_GDPR_LICENSE");
        }
    }

    @Test
    public void testLicenseEmitsWarningOnGracePeriodEnd() {
        LicenseConfiguration.clock = Clock.fixed(expiryDate.plus(1, ChronoUnit.DAYS), ZoneId.of("UTC"));
        LicenseConfiguration actual = LicenseConfiguration.getInstance();
        assertTrue(actual.getExpiryDate().isBefore(LocalDate.now(LicenseConfiguration.clock)));
    }

    @Test
    public void testLicenseThrowsExceptionOnGracePeriodEnd() {
        LicenseConfiguration.clock = Clock.fixed(graceDate.plus(1, ChronoUnit.DAYS), ZoneId.of("UTC"));

        assertThrows(ConfigurationException.class, () -> {
            LicenseConfiguration.getInstance();
        });
    }

    @Test
    public void testInvalidLicenseSignatureThrowsException() {
        System.setProperty("axoniq.dataprotection.license", PATH_TO_INVALID_LICENSE);
        try {
            LicenseConfiguration.getInstance();
            fail("Expected exception");
        } catch (ConfigurationException e) {
            assertTrue(e.getMessage().contains("signature"), "Expected exception message to mention 'signature'");
        }
    }

    @Test
    public void testInvalidLicenseSignatureByFristEnvironmentVariableThrowsException() {
        environmentVariables.set("AXONIQ_DATAPROTECTION_LICENSE", PATH_TO_INVALID_LICENSE);
        try {
            LicenseConfiguration.getInstance();
            fail("Expected exception");
        } catch (ConfigurationException e) {
            assertTrue(e.getMessage().contains("signature"), "Expected exception message to mention 'signature'");
        }
    }

    @Test
    public void testInvalidLicenseSignatureBySecondEnvironmentVariableThrowsException() {
        environmentVariables.set("AXONIQ_GDPR_LICENSE", PATH_TO_INVALID_LICENSE);
        try {
            LicenseConfiguration.getInstance();
            fail("Expected exception");
        } catch (ConfigurationException e) {
            assertTrue(e.getMessage().contains("signature"), "Expected exception message to mention 'signature'");
        }
    }
}