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
import io.axoniq.framework.integrationtests.multitenancy.hunt.SnapshotContentIsolationIT.AddAmount;
import io.axoniq.framework.integrationtests.multitenancy.hunt.SnapshotContentIsolationIT.Ledger;
import io.axoniq.framework.integrationtests.multitenancy.hunt.SnapshotContentIsolationIT.OpenLedger;
import io.axoniq.framework.integrationtests.multitenancy.hunt.SnapshotContentIsolationIT.ReportTotal;
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

/**
 * The single-tenant control arm for {@link SnapshotContentIsolationIT}: the SAME snapshotted entity, the same command
 * flow, on the same Axon Server -- but a plain application on the default context with a directly registered
 * {@link InMemorySnapshotStore}, so the multi-tenant routing engine and its per-tenant snapshot composition are out of
 * the picture.
 * <p>
 * Splits the verdict for the "events after the snapshot are lost when sourcing" observation: green here plus red there
 * indicts the multi-tenant snapshot composition; red here indicts AF5 snapshot sourcing in general.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class SnapshotSingleTenantControlIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String LEDGER_ID = "control-snapshotted-ledger";

    private AxonConfiguration application;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        SnapshotContentIsolationIT.observedTotals().clear();

        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, Ledger.class))
                  .registerCommandHandlingModule(SnapshotContentIsolationIT.ledgerCommandModule())
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
        send(gateway, new OpenLedger(LEDGER_ID));
        for (int i = 0; i < 5; i++) {
            send(gateway, new AddAmount(LEDGER_ID, 1L));
        }

        // and a snapshot actually written (the write is fire-and-forget, during sourcing)
        SnapshotStore store = application.getComponent(SnapshotStore.class);
        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertThat(store.load(new QualifiedName(Ledger.class), LEDGER_ID, null)
                                                    .orTimeout(10, TimeUnit.SECONDS)
                                                    .join())
                       .as("a snapshot must exist after passing the trigger")
                       .isNotNull());

        // then sourcing yields snapshot + every event appended after it
        send(gateway, new ReportTotal(LEDGER_ID));
        assertThat(SnapshotContentIsolationIT.observedTotals().get("<single-tenant>"))
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
