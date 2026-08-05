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

package io.axoniq.framework.integrationtests.multitenancy.hunt;

import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shows why {@link ChaosSingleTenantControlIT} looks broken. On macOS, {@code docker restart} reassigns a
 * container's dynamic Testcontainers port every time instead of keeping it. A client that reads the port once at
 * startup is then stuck retrying a dead port forever, which looks like a permanent recovery failure without being
 * one.
 * <p>
 * Docker on Linux usually keeps ports stable across a plain restart, so this test may legitimately pass there. That
 * would scope the finding to macOS, not contradict it. {@link ChaosSingleTenantControlFixedPortIT} is the other
 * half of the proof: same scenario, stable port, fast recovery.
 */
class DockerRestartPortStabilityIT {

    private static final int RESTARTS = 3;

    private AxonServerContainer container;

    @AfterEach
    void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    void dockerRestartCanReassignADynamicallyPublishedPort() throws Exception {
        container = new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:latest")
                .withAxonServerHostname("localhost")
                .withDevMode(true)
                .withDcbContext(true);
        container.start();
        String containerId = container.getContainerId();

        Set<Integer> observedGrpcPorts = new LinkedHashSet<>();
        observedGrpcPorts.add(publishedGrpcPort(containerId));

        for (int i = 0; i < RESTARTS; i++) {
            restart(containerId);
            observedGrpcPorts.add(publishedGrpcPort(containerId));
        }

        assertThat(observedGrpcPorts)
                .as("gRPC port across %d docker restart(s) of container %s: %s. More than one value means this "
                            + "Docker setup reassigns the port on restart.",
                    RESTARTS, containerId, observedGrpcPorts)
                .hasSize(1);
    }

    private static void restart(String containerId) throws IOException, InterruptedException {
        Process restart = new ProcessBuilder("docker", "restart", containerId).inheritIO().start();
        if (!restart.waitFor(90, TimeUnit.SECONDS) || restart.exitValue() != 0) {
            throw new IllegalStateException("docker restart failed for container " + containerId);
        }
    }

    private static int publishedGrpcPort(String containerId) throws IOException, InterruptedException {
        Process port = new ProcessBuilder("docker", "port", containerId, "8124/tcp").start();
        String output = new String(port.getInputStream().readAllBytes()).trim();
        if (!port.waitFor(10, TimeUnit.SECONDS) || port.exitValue() != 0 || output.isEmpty()) {
            throw new IllegalStateException("Could not read published gRPC port for container " + containerId
                                                     + ": [" + output + "]");
        }
        // output looks like "0.0.0.0:54357"
        return Integer.parseInt(output.substring(output.lastIndexOf(':') + 1));
    }
}
