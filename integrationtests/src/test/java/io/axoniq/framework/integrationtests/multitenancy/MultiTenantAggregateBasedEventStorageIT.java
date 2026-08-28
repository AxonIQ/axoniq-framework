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

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing.AggregateBasedAxonServerTenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import org.axonframework.common.TypeReference;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving that aggregate-based event storage remains isolated per non-DCB tenant context.
 *
 * @author Jan Galinski
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantAggregateBasedEventStorageIT {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String ACCOUNT_ID = "shared-account";

    private final Map<String, TenantBankFixture.BalanceStore> stores = new ConcurrentHashMap<>();

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;
    private String tenantA;
    private String tenantB;

    @BeforeEach
    void setUp() {
        String tenantPrefix = "aggregate-based-" + UUID.randomUUID();
        tenantA = tenantPrefix + "-a";
        tenantB = tenantPrefix + "-b";

        INFRASTRUCTURE.start();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.purgeData();
        contextManager.createContext(tenantA, false);
        contextManager.createContext(tenantB, false);

        application = TenantBankFixture.startApp(
                INFRASTRUCTURE,
                stores,
                tenantPrefix,
                registry -> registry.registerComponent(
                        TenantEventStorageEngineFactory.class,
                        config -> new AggregateBasedAxonServerTenantEventStorageEngineFactory(
                                config.getComponent(AxonServerConnectionManager.class),
                                config.getComponent(EventConverter.class),
                                config.getOptionalComponent(EventTypeResolver.class).orElse(EventTypeResolver.DEFAULT),
                                config.getOptionalComponent(new TypeReference<TenantComponentProvider<Converter>>() {
                                }).orElse(null))
                )
        );
        await().atMost(DEFAULT_TIMEOUT)
               .untilAsserted(() -> assertThat(application.getComponent(TenantProvider.class).tenants())
                       .extracting(TenantDescriptor::tenantId)
                       .contains(tenantA, tenantB));
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.purgeData();
        INFRASTRUCTURE.stop();
    }

    @Test
    void sourcesTheSameAggregateIdentifierFromEachTenantsNonDcbContext() {
        // given
        CommandGateway commands = application.getComponent(CommandGateway.class);

        // when
        dispatch(commands, tenantA, new TenantBankFixture.OpenAccount(ACCOUNT_ID));
        dispatch(commands, tenantA, new TenantBankFixture.DepositMoney(ACCOUNT_ID, 10));
        dispatch(commands, tenantB, new TenantBankFixture.OpenAccount(ACCOUNT_ID));
        dispatch(commands, tenantB, new TenantBankFixture.DepositMoney(ACCOUNT_ID, 20));
        dispatch(commands, tenantA, new TenantBankFixture.RecordBalance(ACCOUNT_ID));
        dispatch(commands, tenantB, new TenantBankFixture.RecordBalance(ACCOUNT_ID));

        // then
        await().atMost(DEFAULT_TIMEOUT)
               .untilAsserted(() -> {
                   assertThat(stores.get(tenantA).observedBalance(ACCOUNT_ID)).isEqualTo(10);
                   assertThat(stores.get(tenantB).observedBalance(ACCOUNT_ID)).isEqualTo(20);
               });
        assertThat(application.getComponent(TenantEventStorageEngineFactory.class))
                .isInstanceOf(AggregateBasedAxonServerTenantEventStorageEngineFactory.class);
    }

    private void dispatch(CommandGateway commands, String tenant, Object command) {
        commands.send(command, TenantBankFixture.tenantMetadata(tenant), null)
                .getResultMessage()
                .orTimeout(DEFAULT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                .join();
    }
}
