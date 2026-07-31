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

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.BalanceStore;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.EmitBalanceUpdate;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.WatchAccount;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.TENANT_A;
import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.TENANT_B;
import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.registerTenantResolver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The in-memory differential arm of hunt scenario S4 (claim MT-C6): same tenant assembly, no Axon Server -- the local
 * {@code SimpleQueryBus} owns the subscription registry. Together with the Axon Server arm this yields the
 * per-backend vector for the rolled-back-emission property.
 */
class InMemoryRolledBackEmissionIT {

    private final Map<String, BalanceStore> stores = new ConcurrentHashMap<>();
    private AxonConfiguration application;

    @BeforeEach
    void setUp() {
        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerCommandHandlingModule(
                          CommandHandlingModule.named("bank-commands")
                                               .commandHandlers()
                                               .autodetectedCommandHandlingComponent(
                                                       c -> new TenantBankFixture.BankCommandHandlers()))
                  .registerQueryHandlingModule(
                          QueryHandlingModule.named("bank-queries")
                                             .queryHandlers()
                                             .autodetectedQueryHandlingComponent(
                                                     c -> new TenantBankFixture.BankQueryHandlers())
                                             .build())
                  .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                  .componentRegistry(registry -> registry
                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                          .disableEnhancer(AxonServerMultiTenancyConfigurationDefaults.class)
                          .disableEnhancer(io.axoniq.license.entitlement.EntitlementConfigurationEnhancer.class)
                          .registerComponent(TenantProvider.class,
                                             config -> new FixedTenantProvider(TENANT_A, TENANT_B)))
                  .componentRegistry(registerTenantResolver(new MetadataBasedTenantResolver()))
                  .componentRegistry(registry -> registry.registerComponent(
                          TenantComponentProvider.class,
                          config -> TenantComponentProvider.withFactory(
                                  BalanceStore.class,
                                  tenant -> stores.computeIfAbsent(tenant.tenantId(), id -> new BalanceStore()))));
        application = configurer.start();
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
    }

    @Test
    void updateEmittedByFailingUnitOfWorkIsNotDelivered() {
        QueryBus queryBus = application.getComponent(QueryBus.class);
        MessageStream<QueryResponseMessage> stream = queryBus.subscriptionQuery(
                new GenericQueryMessage(new MessageType(WatchAccount.class), new WatchAccount("acct-rb"))
                        .andMetadata(tenantMetadata(TENANT_A)),
                null,
                50);
        await().atMost(Duration.ofSeconds(10)).until(stream::hasNextAvailable);
        stream.next(); // initial result

        // when a handler emits an update and then fails its unit of work
        Throwable failure = null;
        try {
            sendAndAwait(new EmitBalanceUpdate("acct-rb", 666L, true));
        } catch (Exception e) {
            failure = e;
        }
        assertThat(failure).as("the emitting command must fail (its handler throws)").isNotNull();

        // and a subsequent successful emission happens
        sendAndAwait(new EmitBalanceUpdate("acct-rb", 1L, false));

        // then the first update observed is the committed one; the rolled-back 666 never surfaces
        await().atMost(Duration.ofSeconds(10)).until(stream::hasNextAvailable);
        Long first = stream.next().orElseThrow().message().payloadAs(Long.class);
        assertThat(first)
                .as("first update after a rolled-back emission followed by a committed emission")
                .isEqualTo(1L);
    }

    private void sendAndAwait(Object command) {
        application.getComponent(CommandGateway.class)
                   .send(command, tenantMetadata(TenantBankFixture.TENANT_A), null)
                   .getResultMessage()
                   .orTimeout(30, TimeUnit.SECONDS)
                   .join();
    }
}
