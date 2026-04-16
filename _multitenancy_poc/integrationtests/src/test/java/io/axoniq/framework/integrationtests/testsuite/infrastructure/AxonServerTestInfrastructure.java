/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.integrationtests.testsuite.infrastructure;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.test.server.AxonServerContainer;
import org.axonframework.test.server.AxonServerContainerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link TestInfrastructure} implementation that wires tests against a real Axon Server instance managed by
 * Testcontainers.
 * <p>
 * The underlying {@link AxonServerContainer} is a {@code static final} field, so it is shared across all instances of
 * this class and all leaf test classes that use it. {@link AxonServerContainer#start()} is idempotent — Testcontainers
 * makes it a no-op when the container is already running — so calling {@link #start()} from every {@code @BeforeEach}
 * is safe and cheap after the first test.
 * <p>
 * Leaf test classes should hold a {@code private static final} instance of this class:
 * <pre>{@code
 * private static final TestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
 *
 * @Override
 * protected TestInfrastructure testInfrastructure() {
 *     return INFRASTRUCTURE;
 * }
 * }</pre>
 *
 * @since 5.1.0
 */
public final class AxonServerTestInfrastructure implements TestInfrastructure {

    private static final Logger LOG = LoggerFactory.getLogger(AxonServerTestInfrastructure.class);

    private static final AxonServerContainer CONTAINER =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:latest")
                    .withAxonServerHostname("localhost")
                    .withDevMode(true)
                    .withReuse(true)
                    .withDcbContext(true);

    @Override
    public void start() {
        boolean wasRunning = CONTAINER.isRunning();
        CONTAINER.start();
        if (!wasRunning) {
            LOG.info("Axon Server UI at http://localhost:{}", CONTAINER.getHttpPort());
        }
    }

    @Override
    public void configureInfrastructure(ComponentRegistry registry) {
        AxonServerConfiguration config = new AxonServerConfiguration();
        config.setServers(CONTAINER.getHost() + ":" + CONTAINER.getGrpcPort());
        registry.registerComponent(AxonServerConfiguration.class, c -> config);
    }

    @Override
    public void purgeData() {
        try {
            LOG.info("Purging events from Axon Server.");
            AxonServerContainerUtils.purgeEventsFromAxonServer(
                    CONTAINER.getHost(),
                    CONTAINER.getHttpPort(),
                    "default",
                    AxonServerContainerUtils.DCB_CONTEXT
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to purge AxonServer event storage", e);
        }
    }

    @Override
    public void stop() {
        // The container is shared across the JVM (static final, withReuse(true)).
        // Testcontainers + Ryuk handle cleanup on JVM exit; stopping per test would
        // defeat reuse. No-op on purpose.
    }
}
