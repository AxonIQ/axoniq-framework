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
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Verifies snapshot sourcing for the ledger fixture with an {@link InMemorySnapshotStore}. */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class SnapshotSingleTenantControlIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.singleTenant();
    private static final String LEDGER_ID = "control-snapshotted-ledger";

    private AxonConfiguration application;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        TenantLedgerFixture.observedTotals().clear();

        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, TenantLedgerFixture.Ledger.class))
                  .registerCommandHandlingModule(TenantLedgerFixture.commandModule())
                  .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                  .componentRegistry(registry -> registry
                          .registerComponent(SnapshotStore.class, c -> new InMemorySnapshotStore())
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
    void eventsAppendedAfterASnapshotAreStillSourcedOnASingleTenantApplication() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);

        // given a ledger past the 3-event snapshot trigger, with 5 amounts of 1 added
        send(gateway, new TenantLedgerFixture.OpenLedger(LEDGER_ID));
        for (int i = 0; i < 5; i++) {
            send(gateway, new TenantLedgerFixture.AddAmount(LEDGER_ID, 1L));
        }

        // and a snapshot actually written (the write is fire-and-forget, during sourcing)
        SnapshotStore store = application.getComponent(SnapshotStore.class);
        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertThat(store.load(new QualifiedName(TenantLedgerFixture.Ledger.class), LEDGER_ID, null)
                                                    .orTimeout(10, TimeUnit.SECONDS)
                                                    .join())
                       .as("a snapshot must exist after passing the trigger")
                       .isNotNull());

        // then sourcing yields snapshot + every event appended after it
        send(gateway, new TenantLedgerFixture.ReportTotal(LEDGER_ID));
        assertThat(TenantLedgerFixture.observedTotals().get("<single-tenant>"))
                .as("single-tenant control: sourcing through a snapshot must still apply the events after it")
                .isEqualTo(5L);
    }

    private static void send(CommandGateway gateway, Object command) {
        gateway.send(command, Metadata.emptyInstance(), null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
