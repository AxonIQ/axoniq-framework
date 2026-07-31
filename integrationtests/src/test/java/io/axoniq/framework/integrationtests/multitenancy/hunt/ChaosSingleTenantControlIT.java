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

import io.axoniq.framework.integrationtests.multitenancy.DisableMultiTenancyTestsWithoutLicense;
import io.axoniq.framework.integrationtests.multitenancy.hunt.SingleTenantControlIT.ControlHandlers;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.Account;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.DepositMoney;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The single-tenant differential arm of {@link ChaosAxonServerRestartIT}: the SAME restart-under-workload chaos on a
 * plain, non-multi-tenant application against the default context. Green here plus red on the multi-tenant arm
 * attributes the missing post-restart recovery to the multi-tenant assembly; red here indicts the connector stack
 * as a whole.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class ChaosSingleTenantControlIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "chaos-control-account";

    private final AtomicReference<String> lastError = new AtomicReference<>("none");
    private AxonConfiguration application;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, Account.class))
                  .registerCommandHandlingModule(
                          CommandHandlingModule.named("control-commands")
                                               .commandHandlers()
                                               .autodetectedCommandHandlingComponent(c -> new ControlHandlers()))
                  .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                  .componentRegistry(registry -> registry
                          .disableEnhancer(io.axoniq.license.entitlement.EntitlementConfigurationEnhancer.class)
                          .disableEnhancer(io.axoniq.license.entitlement.source.axonserver.AxonServerLicenseSourceConfigurationEnhancer.class));
        application = configurer.start();
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        INFRASTRUCTURE.stop();
    }

    @Test
    void singleTenantApplicationRecoversCommandDispatchingAfterAxonServerRestart() throws Exception {
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

            restartAxonServerContainer();
            await().atMost(Duration.ofSeconds(60)).until(() -> unknown.get() > 0);

            long acceptedBeforeRecovery = accepted.get();
            try {
                await().atMost(Duration.ofMinutes(3))
                       .pollInterval(Duration.ofSeconds(1))
                       .until(() -> accepted.get() > acceptedBeforeRecovery + 10);
            } catch (Exception recoveryTimeout) {
                throw new AssertionError("Single-tenant app did not recover within 3 minutes of the restart: accepted="
                                                 + accepted.get() + " unknown=" + unknown.get()
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

    private static void restartAxonServerContainer() throws IOException, InterruptedException {
        Process find = new ProcessBuilder("docker", "ps", "-q", "--filter",
                                          "ancestor=docker.axoniq.io/axoniq/axonserver:latest").start();
        String containerId = new String(find.getInputStream().readAllBytes()).trim().lines().findFirst()
                                                                             .orElseThrow(() -> new IllegalStateException(
                                                                                     "No running Axon Server container found"));
        find.waitFor(10, TimeUnit.SECONDS);
        Process restart = new ProcessBuilder("docker", "restart", containerId).inheritIO().start();
        if (!restart.waitFor(90, TimeUnit.SECONDS) || restart.exitValue() != 0) {
            throw new IllegalStateException("docker restart failed for container " + containerId);
        }
    }
}
