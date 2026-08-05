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
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Verifies that each tenant sources its own snapshotted ledger without losing subsequent events. */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class SnapshotContentIsolationIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String LEDGER_ID = "snapshotted-ledger";

    private final String tenantPrefix = "snapshot-" + Long.toHexString(System.nanoTime());
    private final String tenantA = tenantPrefix + "-a";
    private final String tenantB = tenantPrefix + "-b";

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
        TenantLedgerFixture.observedTotals().clear();

        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, TenantLedgerFixture.Ledger.class))
                  .registerCommandHandlingModule(TenantLedgerFixture.commandModule())
                  .componentRegistry(cr -> cr.registerComponent(TenantResolver.class,
                                                                c -> new MetadataBasedTenantResolver()))
                  .componentRegistry(cr -> cr.registerComponent(TenantConnectPredicate.class,
                                                                c -> d -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT)
                                                                              .contains(d.tenantId())))
                  .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                  .componentRegistry(registry -> registry
                          .disableEnhancer(io.axoniq.license.entitlement.EntitlementConfigurationEnhancer.class)
                          .disableEnhancer(io.axoniq.license.entitlement.source.axonserver.AxonServerLicenseSourceConfigurationEnhancer.class));
        application = configurer.start();
        TenantProvider tenantProvider = application.getComponent(TenantProvider.class);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(tenantProvider.tenants())
                .extracting(TenantDescriptor::tenantId)
                .contains(tenantA, tenantB));
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
    void snapshotsAreWrittenPerTenantAndTheirContentNeverCrossesTenants() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);

        // given the same ledger id in both tenants, past the 3-event snapshot trigger, with distinct amounts
        send(gateway, new TenantLedgerFixture.OpenLedger(LEDGER_ID), tenantA);
        send(gateway, new TenantLedgerFixture.OpenLedger(LEDGER_ID), tenantB);
        for (int i = 0; i < 5; i++) {
            send(gateway, new TenantLedgerFixture.AddAmount(LEDGER_ID, 1L), tenantA);
            send(gateway, new TenantLedgerFixture.AddAmount(LEDGER_ID, 1_000L), tenantB);
        }

        // then each tenant's own snapshot store holds a snapshot for the entity (the write is fire-and-forget)
        TenantSnapshotStoreFactory factory = application.getComponent(TenantSnapshotStoreFactory.class);
        SnapshotStore storeA = factory.storeFor(TenantDescriptor.tenantWithId(tenantA));
        SnapshotStore storeB = factory.storeFor(TenantDescriptor.tenantWithId(tenantB));
        QualifiedName entityName = new QualifiedName(TenantLedgerFixture.Ledger.class);

        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertThat(loadSnapshot(storeA, entityName))
                       .as("tenant A must have a snapshot of its own ledger after passing the trigger")
                       .isNotNull());
        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertThat(loadSnapshot(storeB, entityName))
                       .as("tenant B must have a snapshot of its own ledger after passing the trigger")
                       .isNotNull());
        Snapshot snapshotA = loadSnapshot(storeA, entityName);
        long snapshotTotalA = application.getComponent(GeneralConverter.class)
                                        .convert(snapshotA.payload(), TenantLedgerFixture.Ledger.class)
                                        .getTotal();

        // and the state sourced from that snapshot plus later events is each tenant's own, never the other's
        awaitHandlerRouting(tenantA);
        awaitHandlerRouting(tenantB);
        send(gateway, new TenantLedgerFixture.ReportTotal(LEDGER_ID), tenantA);
        send(gateway, new TenantLedgerFixture.ReportTotal(LEDGER_ID), tenantB);
        assertThat(TenantLedgerFixture.observedTotals().get(tenantA))
                .as("tenant A sourced through its snapshot must see only its 5 x 1 "
                            + "(snapshot total=%s, resume position=%s)", snapshotTotalA, snapshotA.position())
                .isEqualTo(5L);
        assertThat(TenantLedgerFixture.observedTotals().get(tenantB))
                .as("tenant B sourced through its snapshot must see only its 5 x 1000")
               .isEqualTo(5_000L);
    }

    private void awaitHandlerRouting(String tenantId) {
        TenantRouter tenantRouter = application.getComponent(TenantRouter.class);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(tenantRouter.resolveFromMessage(
                new GenericCommandMessage(new MessageType(TenantLedgerFixture.ReportTotal.class),
                                          new TenantLedgerFixture.ReportTotal(LEDGER_ID))
                        .andMetadata(Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY,
                                                   tenantId))))
                .contains(TenantDescriptor.tenantWithId(tenantId)));
    }

    private static Snapshot loadSnapshot(SnapshotStore store, QualifiedName entityName) {
        return store.load(entityName, LEDGER_ID, null)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .join();
    }

    private static void send(CommandGateway gateway, Object command, String tenant) {
        gateway.send(command,
                     Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, tenant),
                     null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
