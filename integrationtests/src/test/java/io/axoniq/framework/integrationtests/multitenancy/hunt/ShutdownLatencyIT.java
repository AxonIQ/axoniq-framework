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
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hunt scenario on claim MT-C8 / gap MT-M4: shutting down an IDLE multi-tenant application must complete within the
 * framework's own per-phase budget (5s), like any other application. During the scenario batteries every single
 * multi-tenant application shutdown logged "Timed out during shutdown phase [536870911]" (the inbound-connector
 * phase), leaking live per-tenant connectors into subsequent tests.
 * <p>
 * A watchdog dumps all thread stacks to stderr when shutdown exceeds 4s, so a red run carries the blocking frame.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class ShutdownLatencyIT {

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    private final Map<String, BalanceStore> stores = new ConcurrentHashMap<>();
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
        if (application != null) {
            application.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void idleMultiTenantApplicationShutsDownWithinThePhaseBudget() throws InterruptedException {
        // given an idle two-tenant application that has fully started (setUp), with no in-flight work

        // when shutting it down, with a watchdog capturing stacks if it stalls
        CountDownLatch done = new CountDownLatch(1);
        Thread watchdog = new Thread(() -> {
            try {
                if (!done.await(4, TimeUnit.SECONDS)) {
                    System.err.println("=== shutdown stalled >4s; thread dump ===");
                    for (ThreadInfo info : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
                        System.err.print(info.toString());
                    }
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown-watchdog");
        watchdog.start();

        long start = System.nanoTime();
        application.shutdown();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        done.countDown();
        watchdog.join();
        application = null;

        // then the shutdown completes inside the framework's own 5s phase budget: reaching that budget means a
        // shutdown handler never completed and was abandoned, leaving live per-tenant connectors behind
        assertThat(elapsedMs)
                .as("idle multi-tenant application shutdown duration (>=5000ms means a phase timed out)")
                .isLessThan(5_000L);
    }
}
