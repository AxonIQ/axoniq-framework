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

    // All overloads funnel into the same DockerImageName constructor, so only image-name resolution differs here;
    // actually starting a container is covered by the tests below.
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
    void constructionWithDCBStartsAsExpected() {
        String testName = "axoniq/axonserver";
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
    void properlyConfiguredDefaultContainerLabel() {
        try (AxonServerContainer testSubject = new AxonServerContainer()) {
            assertThat(testSubject.getDockerImageName()).isEqualTo("docker.axoniq.io/axoniq/axonserver:latest");
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
