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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hunt scenario S8 (claims MT-C4, MT-C5): the same entity id is used concurrently in two tenants; each tenant's
 * sourced state must equal the reference state computed from that tenant's own accepted commands, and no event of one
 * tenant may influence the other's sourced stream.
 * <p>
 * The oracle is interleaving-insensitive: per-tenant conservation (observed sourced balance == sum of that tenant's
 * accepted deposit amounts).
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class PerTenantEventSourcingIsolationIT {

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "shared-account-id";
    private static final int DEPOSITS_PER_TENANT = 50;

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
        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder(ADMIN_CONTEXT, DEFAULT_CONTEXT, tenantA, tenantB);
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
    void sameEntityIdInTwoTenantsKeepsPerTenantConservationUnderConcurrentLoad() throws Exception {
        CommandGateway gateway = application.getComponent(CommandGateway.class);

        // given the same account id opened in both tenants
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID), tenantA);
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID), tenantB);

        // when depositing concurrently into both tenants from four dispatcher threads
        AtomicLong acceptedSumA = new AtomicLong();
        AtomicLong acceptedSumB = new AtomicLong();
        List<CompletableFuture<Void>> dispatches = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            for (int i = 1; i <= DEPOSITS_PER_TENANT; i++) {
                long amountA = i;
                long amountB = i * 1_000L;
                dispatches.add(CompletableFuture.runAsync(
                        () -> deposit(gateway, amountA, tenantA, acceptedSumA), pool));
                dispatches.add(CompletableFuture.runAsync(
                        () -> deposit(gateway, amountB, tenantB, acceptedSumB), pool));
            }
            CompletableFuture.allOf(dispatches.toArray(CompletableFuture[]::new))
                             .orTimeout(120, TimeUnit.SECONDS)
                             .join();
        }

        // then both tenants accepted work (fault-landing evidence for the concurrency claim)
        assertThat(acceptedSumA.get()).isPositive();
        assertThat(acceptedSumB.get()).isPositive();

        // and the sourced balance of each tenant equals exactly the sum of that tenant's accepted deposits
        sendAndAwait(gateway, new RecordBalance(ACCOUNT_ID), tenantA);
        sendAndAwait(gateway, new RecordBalance(ACCOUNT_ID), tenantB);
        assertThat(stores.get(tenantA).observedBalance(ACCOUNT_ID))
                .as("tenant A sourced balance vs sum of tenant A accepted deposits")
                .isEqualTo(acceptedSumA.get());
        assertThat(stores.get(tenantB).observedBalance(ACCOUNT_ID))
                .as("tenant B sourced balance vs sum of tenant B accepted deposits")
                .isEqualTo(acceptedSumB.get());
    }

    private void deposit(CommandGateway gateway, long amount, String tenant, AtomicLong acceptedSum) {
        // A deposit rejected by a concurrent-modification conflict is retried; after three failures it is counted as
        // not accepted, which the conservation oracle then does not expect in the balance.
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                sendAndAwait(gateway, new DepositMoney(ACCOUNT_ID, amount), tenant);
                acceptedSum.addAndGet(amount);
                return;
            } catch (Exception e) {
                if (attempt == 2) {
                    return;
                }
            }
        }
    }

    private static void sendAndAwait(CommandGateway gateway, Object command, String tenant) {
        gateway.send(command, tenantMetadata(tenant), null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
