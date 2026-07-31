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
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.ChainDeposit;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.RecordBalance;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.correlation.CorrelationDataProviderRegistry;
import org.axonframework.messaging.core.correlation.SimpleCorrelationDataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hunt scenario S2 (claim MT-C1, gap MT-M3): "a command dispatched while handling another message stays with the
 * tenant of that message instead of having to name its tenant again" (per-tenant connector Javadoc).
 * <p>
 * The outer {@link ChainDeposit} names tenant A through metadata. Its handler dispatches a follow-up
 * {@link TenantBankFixture.DepositMoney} through the context-bound {@code CommandDispatcher} without naming any
 * tenant. The claim holds if the follow-up is handled with tenant A resolved: its handler's tenant-scoped store
 * resolves, and the deposited amount is present in tenant A's sourced balance afterwards.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class TenantPropagationAcrossHopsIT {

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "chain-account";

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
    void followUpCommandDispatchedInsideHandlerStaysWithTheTenantOfTheHandledMessage() {
        application = TenantBankFixture.startApp(INFRASTRUCTURE, stores, runId, registry -> {
        });
        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID), tenantA);

        // when the tenant-A handler dispatches a follow-up deposit without naming its tenant
        sendAndAwait(gateway, new ChainDeposit(ACCOUNT_ID, 42L), tenantA);

        // then the follow-up was handled with tenant A (its handler recorded success, not a resolution failure)
        assertThat(stores.get(tenantA))
                .as("tenant A store must exist: the outer handler is tenant-scoped")
                .isNotNull();
        assertThat(stores.get(tenantA).note("chain-" + ACCOUNT_ID))
                .as("the follow-up DepositMoney dispatched without tenant metadata, from inside a tenant-A handler")
                .isEqualTo("ok");

        // and the deposited amount is in tenant A's sourced balance
        sendAndAwait(gateway, new RecordBalance(ACCOUNT_ID), tenantA);
        assertThat(stores.get(tenantA).observedBalance(ACCOUNT_ID)).isEqualTo(42L);
    }

    /**
     * The candidate-fix arm: with a {@code SimpleCorrelationDataProvider("tenantId")} registered -- the combination
     * the {@code MetadataBasedTenantResolver} Javadoc describes but no enhancer registers -- the same chain works.
     * Green here plus red above pins the missing default as the cause.
     */
    @Test
    void followUpCommandKeepsItsTenantWhenATenantCorrelationProviderIsRegistered() {
        application = TenantBankFixture.startApp(
                INFRASTRUCTURE,
                stores,
                registry -> registry.registerDecorator(
                        CorrelationDataProviderRegistry.class,
                        0,
                        (config, name, delegate) -> delegate.registerProvider(
                                c -> new SimpleCorrelationDataProvider(
                                        MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY))));
        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID), tenantA);

        sendAndAwait(gateway, new ChainDeposit(ACCOUNT_ID, 42L), tenantA);

        assertThat(stores.get(tenantA).note("chain-" + ACCOUNT_ID)).isEqualTo("ok");
        sendAndAwait(gateway, new RecordBalance(ACCOUNT_ID), tenantA);
        assertThat(stores.get(tenantA).observedBalance(ACCOUNT_ID)).isEqualTo(42L);
    }

    private static void sendAndAwait(CommandGateway gateway, Object command, String tenant) {
        gateway.send(command, tenantMetadata(tenant), null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
