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
 * Direct evidence for the root cause behind the apparent non-recovery in {@link ChaosSingleTenantControlIT}: on this
 * machine, {@code docker restart} on a container published with Testcontainers' default DYNAMIC port allocation
 * reassigns a brand-new random host port every time, rather than keeping the one the client discovered at startup.
 * A client that caches that port once (as every chaos test here does) is then structurally stuck retrying a port
 * nothing is listening on, for as long as the test is willing to wait -- which looks exactly like "command dispatch
 * never recovers" without actually being one.
 * <p>
 * This is environment-dependent (observed on macOS with Docker Desktop). Docker on Linux traditionally keeps a
 * container's iptables-based port bindings stable across a plain restart (only a full recreate reassigns them), so
 * this test may legitimately pass with a single stable port there -- that outcome does not contradict the finding,
 * it scopes it. Either way, the result tells you directly whether this CI/dev machine is exposed to the failure mode
 * {@link ChaosSingleTenantControlIT} reports, instead of leaving it to be inferred.
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

        // then: a client that discovered the port once, before any restart, would be pointed at a dead port for
        // every restart that reassigned it. A single, stable port across all restarts is the only outcome under
        // which the chaos tests' "record the port once at startup" approach is safe.
        assertThat(observedGrpcPorts)
                .as("gRPC port observed before and after %d docker restart(s) of container %s: %s. More than one "
                            + "distinct value means this Docker setup reassigns published ports across a plain "
                            + "restart, which is what makes ChaosSingleTenantControlIT look like a permanent "
                            + "command-dispatch failure.",
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
