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
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hunt scenario on claim MT-C5 ("Both stay within one tenant" -- per-tenant snapshot composition), end to end on real
 * Axon Server rather than at unit level.
 * <p>
 * ORACLE: the {@link TenantSnapshotStoreFactory} hands out one {@link SnapshotStore} per tenant, distinct instances per
 * tenant id, and the store handed out for a tenant is never shared with another tenant; sourcing after activity in
 * both tenants keeps each tenant's balance equal to its own deposits (a snapshot leaking across tenants would show up
 * as a foreign balance).
 * <p>
 * The engine-level composition (engine + that tenant's snapshot store) is exercised by driving real commands: every
 * sourcing goes through {@code SnapshotCapableEventStorageEngine.decorate(engineFor(tenant), storeFor(tenant))}.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class PerTenantSnapshotIsolationIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "snapshot-account";

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

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
    void eachTenantGetsItsOwnSnapshotStoreAndNoSnapshotCrossesTenants() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);

        // given the same account id active in both tenants, with different amounts
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID), tenantA);
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID), tenantB);
        for (int i = 0; i < 5; i++) {
            sendAndAwait(gateway, new DepositMoney(ACCOUNT_ID, 1L), tenantA);
            sendAndAwait(gateway, new DepositMoney(ACCOUNT_ID, 1_000L), tenantB);
        }

        // then the snapshot store factory yields a DISTINCT store per tenant, and never the same instance
        TenantSnapshotStoreFactory factory = application.getComponent(TenantSnapshotStoreFactory.class);
        SnapshotStore storeA = factory.storeFor(TenantDescriptor.tenantWithId(tenantA));
        SnapshotStore storeB = factory.storeFor(TenantDescriptor.tenantWithId(tenantB));
        assertThat(storeA).as("tenant A's snapshot store").isNotNull();
        assertThat(storeB).as("tenant B's snapshot store").isNotNull();
        assertThat(storeA)
                .as("a snapshot store serving two tenants would break per-tenant snapshot isolation")
                .isNotSameAs(storeB);
        // and the store is stable per tenant (a fresh store per call would make snapshots unfindable)
        assertThat(factory.storeFor(TenantDescriptor.tenantWithId(tenantA))).isSameAs(storeA);
        assertThat(Set.of(storeA, storeB)).hasSize(2);

        // and sourcing through each tenant's composed engine yields that tenant's own state only
        sendAndAwait(gateway, new RecordBalance(ACCOUNT_ID), tenantA);
        sendAndAwait(gateway, new RecordBalance(ACCOUNT_ID), tenantB);
        assertThat(stores.get(tenantA).observedBalance(ACCOUNT_ID))
                .as("tenant A's sourced balance must be its own 5 deposits of 1")
                .isEqualTo(5L);
        assertThat(stores.get(tenantB).observedBalance(ACCOUNT_ID))
                .as("tenant B's sourced balance must be its own 5 deposits of 1000")
                .isEqualTo(5_000L);
    }

    private void sendAndAwait(CommandGateway gateway, Object command, String tenant) {
        gateway.send(command, tenantMetadata(tenant), null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
