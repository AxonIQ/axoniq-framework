/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.testcontainer;

import org.junit.jupiter.api.*;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link AxonServerContainer}'s builder/configuration logic directly, without starting a real container:
 * {@link AxonServerContainer#configure()} applies builder state to the container definition before Testcontainers
 * ever talks to Docker, so calling it directly is enough to verify the wiring. Startup/reuse behavior itself is
 * covered by {@link AxonServerContainerIT}.
 *
 * @author Lucas Campos
 * @author Steven van Beelen
 */
class AxonServerContainerTest {

    @Test
    void constructionOverloadsResolveToEquivalentConfiguration() {
        try (
                AxonServerContainer viaString = new AxonServerContainer("axoniq/axonserver");
                AxonServerContainer viaDockerImageName = new AxonServerContainer(DockerImageName.parse("axoniq/axonserver"));
                AxonServerContainer viaDefaultConstructor = new AxonServerContainer()
        ) {
            assertThat(viaString.getDockerImageName()).isEqualTo(viaDockerImageName.getDockerImageName());
            assertThat(viaDefaultConstructor.getDockerImageName()).isEqualTo("docker.axoniq.io/axoniq/axonserver:latest");
        }
    }

    @Test
    void properlyConfiguredDefaultContainerLabel() {
        try (AxonServerContainer testSubject = new AxonServerContainer()) {
            assertThat(testSubject.getDockerImageName()).isEqualTo("docker.axoniq.io/axoniq/axonserver:latest");
        }
    }

    @Test
    void withDcbContextSetsTheFlag() {
        try (AxonServerContainer testSubject = new AxonServerContainer().withDcbContext(true)) {
            assertThat(testSubject.isDcbContext()).isTrue();
        }
    }

    @Test
    void configureAppliesCustomizedSettingsToTheEnvironment() {
        String testName = "axon-server-name";
        String testHostName = "axon-server-hostname";
        String testInternalHostName = "axon-server-internal-host-name";
        boolean testDevMode = true;
        try (
                AxonServerContainer testSubject =
                        new AxonServerContainer("axoniq/axonserver")
                                .withAxonServerName(testName)
                                .withAxonServerHostname(testHostName)
                                .withAxonServerInternalHostname(testInternalHostName)
                                .withDevMode(testDevMode)
        ) {
            testSubject.configure();

            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_NAME")).isEqualTo(testName);
            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_INTERNAL_HOSTNAME")).isEqualTo(
                    testInternalHostName);
            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_HOSTNAME")).isEqualTo(testHostName);
            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_DEVMODE_ENABLED")).isEqualTo(Boolean.toString(
                    testDevMode));
        }
    }

    @Nested
    class License {

        @Test
        void configurePrefersLicenseContentOverLicensePath() {
            try (
                    AxonServerContainer testSubject = new AxonServerContainer()
                            .withLicense("axon-server-test.license")
                            .withLicenseContent("license-content")
            ) {
                // when
                testSubject.configure();

                // then
                // The license path points to a resource that isn't on this test classpath, so falling back to it
                // would already fail configure(); the absent file mapping confirms it was not used.
                assertThat(testSubject.getCopyToFileContainerPathMap())
                        .doesNotContainValue("/axonserver/config/axoniq.license");
            }
        }

        @Test
        void toStringDoesNotExposeLicenseContent() {
            try (AxonServerContainer testSubject = new AxonServerContainer().withLicenseContent("secret-license")) {
                // when
                String result = testSubject.toString();

                // then
                assertThat(result).doesNotContain("secret-license");
            }
        }
    }
}
