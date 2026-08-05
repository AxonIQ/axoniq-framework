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

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.integrationtests.multitenancy.hunt.SingleTenantControlIT.ControlHandlers;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.Account;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.DepositMoney;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves AxoniQ/axoniq-framework#320 is not a connector bug. Same app, same {@link ControlHandlers}, same
 * workload, same timeouts as {@link ChaosSingleTenantControlIT}. Only the Axon Server port is fixed instead of
 * dynamic. Recovery happens in seconds. See {@link DockerRestartPortStabilityIT} for why the port matters.
 * <p>
 * Uses its own container on hardcoded fixed ports instead of the shared
 * {@link io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure} instance, so
 * it does not conflict with that instance's dynamically-ported container. Do not run concurrently with another
 * instance of this test.
 */
class ChaosSingleTenantControlFixedPortIT {

    private static final int FIXED_HTTP_PORT = 19024;
    private static final int FIXED_GRPC_PORT = 19124;
    private static final String ACCOUNT_ID = "chaos-control-account";

    private final AtomicReference<String> lastError = new AtomicReference<>("none");
    private FixedPortAxonServerContainer container;
    private AxonConfiguration application;

    /** Exposes {@code addFixedExposedPort}, which {@link AxonServerContainer} does not itself publish. */
    private static final class FixedPortAxonServerContainer extends AxonServerContainer {

        FixedPortAxonServerContainer() {
            super("docker.axoniq.io/axoniq/axonserver:latest");
            addFixedExposedPort(FIXED_HTTP_PORT, 8024);
            addFixedExposedPort(FIXED_GRPC_PORT, 8124);
        }
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        if (container != null) {
            container.stop();
        }
    }

    @Test
    void singleTenantApplicationRecoversCommandDispatchingAfterAxonServerRestart() throws Exception {
        container = new FixedPortAxonServerContainer();
        container.withAxonServerHostname("localhost");
        container.withDevMode(true);
        container.withDcbContext(true);
        container.start();
        String containerId = container.getContainerId();

        AxonServerConfiguration.Builder configBuilder = builder().servers("localhost:" + FIXED_GRPC_PORT);
        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, Account.class))
                  .registerCommandHandlingModule(
                          CommandHandlingModule.named("control-commands")
                                               .commandHandlers()
                                               .autodetectedCommandHandlingComponent(c -> new ControlHandlers()))
                  .componentRegistry(registry -> registry.registerComponent(AxonServerConfiguration.class,
                                                                            c -> configBuilder.build()))
                  .componentRegistry(registry -> registry
                          .disableEnhancer(io.axoniq.license.entitlement.EntitlementConfigurationEnhancer.class)
                          .disableEnhancer(io.axoniq.license.entitlement.source.axonserver.AxonServerLicenseSourceConfigurationEnhancer.class));
        application = configurer.start();

        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID));

        AtomicLong accepted = new AtomicLong();
        AtomicLong unknown = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> {
                    while (!stop.get()) {
                        try {
                            sendAndAwait(gateway, new DepositMoney(ACCOUNT_ID, 1L));
                            accepted.incrementAndGet();
                        } catch (Exception e) {
                            unknown.incrementAndGet();
                            Throwable cause = e.getCause() != null ? e.getCause() : e;
                            lastError.set(cause.getClass().getSimpleName() + ": " + cause.getMessage());
                            LockSupport.parkNanos(50_000_000L);
                        }
                    }
                });
            }
            await().atMost(Duration.ofSeconds(30)).until(() -> accepted.get() > 10);

            restartAxonServerContainer(containerId);
            await().atMost(Duration.ofSeconds(60)).until(() -> unknown.get() > 0);

            long acceptedBeforeRecovery = accepted.get();
            try {
                await().atMost(Duration.ofMinutes(3))
                       .pollInterval(Duration.ofSeconds(1))
                       .until(() -> accepted.get() > acceptedBeforeRecovery + 10);
            } catch (Exception recoveryTimeout) {
                throw new AssertionError("Fixed-port single-tenant app did not recover within 3 minutes of the "
                                                 + "restart: accepted=" + accepted.get() + " unknown=" + unknown.get()
                                                 + " lastWorkloadFailure=" + lastError.get(), recoveryTimeout);
            }
        } finally {
            stop.set(true);
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void sendAndAwait(CommandGateway gateway, Object command) {
        gateway.send(command, Metadata.emptyInstance(), null)
               .getResultMessage()
               .orTimeout(10, TimeUnit.SECONDS)
               .join();
    }

    // Unlike ChaosSingleTenantControlIT, this restarts by the id of the container this test itself started, rather
    // than looking one up with `docker ps --filter ancestor=...`. That filter would match any container running
    // the same image, which is fragile whenever more than one is present, and this test's container shares its
    // image with the one ChaosSingleTenantControlIT uses.
    private static void restartAxonServerContainer(String containerId) throws IOException, InterruptedException {
        Process restart = new ProcessBuilder("docker", "restart", containerId).inheritIO().start();
        if (!restart.waitFor(90, TimeUnit.SECONDS) || restart.exitValue() != 0) {
            throw new IllegalStateException("docker restart failed for container " + containerId);
        }
    }
}
