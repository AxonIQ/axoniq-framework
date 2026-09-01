/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.testcontainer;

import org.junit.jupiter.api.*;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple test class for {@link AxonServerContainer}.
 *
 * @author Lucas Campos
 * @author Steven van Beelen
 */
@Tag("nightly")
@Tag("slow")
@Tag("flaky")
class AxonServerContainerIT {

    @Test
    void constructionThroughStringImageNameStartsAsExpected() {
        String testName = "axoniq/axonserver";
        try (
                AxonServerContainer testSubject = new AxonServerContainer(testName)
        ) {
            testSubject.start();
            assertThat(testSubject.isRunning()).isTrue();
        }
    }

    @Test
    void constructionWithDCBStartsAsExpected() {
        // Pinned: axonserver:latest (2026.1.1) raises java.lang.VerifyError during DCB context creation.
        String testName = "docker.axoniq.io/axoniq/axonserver:2026.1.0";
        try (
                AxonServerContainer testSubject = new AxonServerContainer(testName)
                        .withDcbContext(true)
        ) {
            testSubject.start();
            assertThat(testSubject.isRunning()).isTrue();
        }
    }

    @Test
    void constructionThroughDockerImageNameStartsAsExpected() {
        DockerImageName testName = DockerImageName.parse("axoniq/axonserver");
        try (
                AxonServerContainer testSubject = new AxonServerContainer(testName)
        ) {
            testSubject.start();
            assertThat(testSubject.isRunning()).isTrue();
            assertThat(testSubject.getAxonServerAddress()).isNotNull();
            assertThat(testSubject.getGrpcPort()).isNotNull();
            assertThat(testSubject.getHost()).isNotNull();
            assertThat(testSubject.getExposedPorts()).contains(8024, 8124);
        }
    }

    @Test
    void fullyCustomizedAxonServerContainerStartsAsExpected() {
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
            testSubject.start();
            assertThat(testSubject.isRunning()).isTrue();

            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_NAME")).isEqualTo(testName);
            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_INTERNAL_HOSTNAME")).isEqualTo(
                    testInternalHostName);
            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_HOSTNAME")).isEqualTo(testHostName);
            assertThat(testSubject.getEnvMap().get("AXONIQ_AXONSERVER_DEVMODE_ENABLED")).isEqualTo(Boolean.toString(
                    testDevMode));
        }
    }

    @Test
    void constructionThroughDefaultConstructorStartsAsExpected() {
        try (AxonServerContainer testSubject = new AxonServerContainer()) {
            testSubject.start();
            assertThat(testSubject.isRunning()).isTrue();
        }
    }

    @Test
    void properlyConfiguredDefaultContainerLabel() {
        try (AxonServerContainer testSubject = new AxonServerContainer()) {
            assertThat(testSubject.getDockerImageName()).isEqualTo("docker.axoniq.io/axoniq/axonserver:2026.1.0");
        }
    }

    @Test
    void constructionWithReuseEnabledStartsMultipleTimesAsExpected() {
        try (AxonServerContainer testSubject = new AxonServerContainer().withReuse(true)) {

            testSubject.doStart();
            assertThat(testSubject.isRunning()).isTrue();

            testSubject.doStart();
            assertThat(testSubject.isRunning()).isTrue();
        }
    }
}
