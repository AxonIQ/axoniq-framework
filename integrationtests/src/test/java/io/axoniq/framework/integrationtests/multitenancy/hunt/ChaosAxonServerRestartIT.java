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
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.BalanceStore;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.DepositMoney;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.RecordBalance;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Chaos arm over claims MT-C4 (per-tenant isolation) and the durability of accepted work: Axon Server is restarted in
 * the middle of a two-tenant deposit workload.
 * <p>
 * Outcome classification is three-valued, per the hunt constitution: a deposit whose dispatch future completes
 * normally is ACCEPTED; one that fails or times out is UNKNOWN (it may or may not have been persisted -- a response
 * lost to the restart does not un-append the event); unknowns are never collapsed into either side of the oracle.
 * <p>
 * Oracle, per tenant (tenant A deposits are always amount 1, tenant B always 1000, both to the SAME account id):
 * {@code accepted <= balance <= accepted + unknown}, and tenant A's balance stays below 1000 -- a single tenant-B
 * event bleeding into tenant A's stream trips it immediately.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class ChaosAxonServerRestartIT {

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "chaos-account";
    private static final long AMOUNT_A = 1L;
    private static final long AMOUNT_B = 1_000L;

    private final Map<String, BalanceStore> stores = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicReference<String> lastError =
            new java.util.concurrent.atomic.AtomicReference<>("none");
    private AxonConfiguration application;
    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        contextManager.createContext(tenantA);
        contextManager.createContext(tenantB);
        application = TenantBankFixture.startApp(INFRASTRUCTURE, stores, runId, registry -> {
        });
    }

    @AfterEach
    void tearDown() {
        application.shutdown();
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void perTenantConservationHoldsAcrossAnAxonServerRestart() throws Exception {
        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendWithRetry(gateway, new OpenAccount(ACCOUNT_ID), tenantA);
        sendWithRetry(gateway, new OpenAccount(ACCOUNT_ID), tenantB);

        AtomicLong acceptedA = new AtomicLong();
        AtomicLong acceptedB = new AtomicLong();
        AtomicLong unknownA = new AtomicLong();
        AtomicLong unknownB = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch workloadStarted = new CountDownLatch(2);
        lastError.set("none");

        // when a deposit workload runs against both tenants while Axon Server restarts underneath it
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> workload(gateway, tenantA, AMOUNT_A, acceptedA, unknownA, stop, workloadStarted));
                pool.submit(() -> workload(gateway, tenantB, AMOUNT_B, acceptedB, unknownB, stop, workloadStarted));
            }

            assertThat(workloadStarted.await(30, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(3_000);

            restartAxonServerContainer();

            // fault landing evidence: the restart must be visible to the workload as failures/unknown outcomes
            await().atMost(Duration.ofSeconds(60))
                   .until(() -> unknownA.get() + unknownB.get() > 0);

            // recovery: keep the workload running until commands succeed again after the restart. On timeout the
            // captured last workload failure names the reason recovery never happened.
            long acceptedBeforeRecovery = acceptedA.get() + acceptedB.get();
            try {
                await().atMost(Duration.ofMinutes(3))
                       .pollInterval(Duration.ofSeconds(1))
                       .until(() -> acceptedA.get() + acceptedB.get() > acceptedBeforeRecovery + 10);
            } catch (Exception recoveryTimeout) {
                throw new AssertionError("No recovery within 3 minutes of the Axon Server restart: accepted="
                                                 + (acceptedA.get() + acceptedB.get())
                                                 + " unknown=" + (unknownA.get() + unknownB.get())
                                                 + " lastWorkloadFailure=" + lastError.get(), recoveryTimeout);
            }
        } finally {
            // The workers must stop even when an await above fails, or closing the pool would hang forever.
            stop.set(true);
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }

        // then, per tenant: accepted <= balance <= accepted + unknown, in that tenant's own value space
        long balanceA = recordedBalance(gateway, tenantA);
        long balanceB = recordedBalance(gateway, tenantB);

        assertThat(balanceA % AMOUNT_A).isZero();
        assertThat(balanceB % AMOUNT_B)
                .as("tenant B's balance must consist of tenant-B amounts only (bleed check)")
                .isZero();
        assertThat(balanceA)
                .as("tenant A: accepted=%s unknown=%s -- balance below accepted means accepted work was lost; "
                            + "above accepted+unknown means duplicated or foreign events",
                    acceptedA.get(), unknownA.get())
                .isBetween(acceptedA.get(), acceptedA.get() + unknownA.get() * AMOUNT_A)
                .isLessThan(AMOUNT_B); // a single tenant-B event in tenant A's stream would exceed this
        assertThat(balanceB)
                .as("tenant B: accepted=%s unknown=%s", acceptedB.get(), unknownB.get())
                .isBetween(acceptedB.get(), acceptedB.get() + unknownB.get() * AMOUNT_B);
    }

    private void workload(CommandGateway gateway,
                          String tenant,
                          long amount,
                          AtomicLong accepted,
                          AtomicLong unknown,
                          AtomicBoolean stop,
                          CountDownLatch started) {
        started.countDown();
        while (!stop.get()) {
            try {
                gateway.send(new DepositMoney(ACCOUNT_ID, amount), tenantMetadata(tenant), null)
                       .getResultMessage()
                       .orTimeout(10, TimeUnit.SECONDS)
                       .join();
                accepted.addAndGet(amount);
            } catch (Exception e) {
                // The outcome is unknown: the event may have been persisted even though the response was lost.
                unknown.incrementAndGet();
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                lastError.set(cause.getClass().getSimpleName() + ": " + cause.getMessage());
                // Back off briefly so an instantly-failing dispatch does not busy-spin the workload threads.
                LockSupport.parkNanos(50_000_000L);
            }
        }
    }

    private long recordedBalance(CommandGateway gateway, String tenant) {
        sendWithRetry(gateway, new RecordBalance(ACCOUNT_ID), tenant);
        return stores.get(tenant).observedBalance(ACCOUNT_ID);
    }

    private static void sendWithRetry(CommandGateway gateway, Object command, String tenant) {
        await().atMost(Duration.ofMinutes(2))
               .pollInterval(Duration.ofSeconds(1))
               .until(() -> {
                   try {
                       gateway.send(command, tenantMetadata(tenant), null)
                              .getResultMessage()
                              .orTimeout(15, TimeUnit.SECONDS)
                              .join();
                       return true;
                   } catch (Exception e) {
                       return false;
                   }
               });
    }

    private static void restartAxonServerContainer() throws IOException, InterruptedException {
        Process find = new ProcessBuilder("docker", "ps", "-q", "--filter", "ancestor=docker.axoniq.io/axoniq/axonserver:latest")
                .start();
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
