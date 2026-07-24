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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.RecordingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that snapshotting still works, per tenant, when it goes through the standard event-sourcing wiring rather than
 * the routing snapshot store directly.
 * <p>
 * With the event-sourcing defaults active, the registered multi-tenant {@link EventStorageEngine} is decorated with a
 * {@link SnapshotCapableEventStorageEngine} (because it does not implement {@link SnapshotStore} itself). The optimized
 * snapshot strategy then loads the snapshot through the routing {@link MultiTenantSnapshotStore} and delegates the
 * event tail to the routing engine, both resolved to the same tenant from the {@link ProcessingContext}. So a snapshot
 * stored for one tenant is used only when sourcing that tenant, and a tenant without one falls back to a snapshot-free
 * stream without ever touching another tenant's store.
 *
 * @author Laura Devriendt
 */
class PerTenantSnapshotSourcingIsolationTest {

    private static final QualifiedName SNAPSHOT_NAME = new QualifiedName(String.class);
    private static final String IDENTIFIER = "course-1";

    private final TenantDescriptorMapping<EventStorageEngine> tenantEngines = new TenantDescriptorMapping<>();
    private final TenantDescriptorMapping<SnapshotStore> tenantSnapshotStores = new TenantDescriptorMapping<>();
    private final RecordingSnapshotStore snapshotStoreA =
            tenantSnapshotStores.entry(TENANT_A, new RecordingSnapshotStore());
    private final RecordingSnapshotStore snapshotStoreB =
            tenantSnapshotStores.entry(TENANT_B, new RecordingSnapshotStore());

    private AxonConfiguration configuration;
    private EventStorageEngine sourcingEngine;

    @BeforeEach
    void setUp() {
        tenantEngines.entry(TENANT_A, new RecordingEventStorageEngine());
        tenantEngines.entry(TENANT_B, new RecordingEventStorageEngine());
        EventStorageEngine routingEngine = new MultiTenantEventStorageEngine(
                tenantEngines::apply, new MetadataBasedTenantResolver(), tenantEngines);
        SnapshotStore routingSnapshotStore = new MultiTenantSnapshotStore(
                tenantSnapshotStores::apply, new MetadataBasedTenantResolver(), tenantSnapshotStores);

        StubTenantProvider tenantProvider = new StubTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        tenantProvider.addTenant(TENANT_B);

        configuration = EventSourcingConfigurer
                .create()
                .componentRegistry(registry -> {
                    MultiTenancyEnabled.enableMultiTenancyEnhancer(registry);
                    // Register the multi-tenant engine and snapshot store as the Axon Server defaults would, but backed
                    // by recording per-tenant doubles and without reaching a real Axon Server.
                    registry.disableEnhancer(AxonServerConfigurationEnhancer.class)
                            .disableEnhancer(AxonServerMultiTenancyConfigurationDefaults.class)
                            .registerComponent(TenantProvider.class, config -> tenantProvider)
                            .registerComponent(EventStorageEngine.class, config -> routingEngine)
                            .registerComponent(SnapshotStore.class, config -> routingSnapshotStore);
                })
                .build();
        configuration.start();
        sourcingEngine = configuration.getComponent(EventStorageEngine.class);
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void theMultiTenantEngineIsDecoratedToBeSnapshotCapable() {
        assertThat(sourcingEngine).isInstanceOf(SnapshotCapableEventStorageEngine.class);
    }

    @Test
    void snapshotSourcingLoadsTheSnapshotFromTheSourcedTenantsOwnStore() {
        Snapshot snapshotA = snapshot("snapshot-a");
        snapshotStoreA.store(SNAPSHOT_NAME, IDENTIFIER, snapshotA, null).join();

        Snapshot sourcedForA = leadingSnapshot(sourcingEngine.source(snapshotCondition(), contextFor(TENANT_A)));

        assertThat(sourcedForA).isEqualTo(snapshotA);
        assertThat(snapshotStoreA.loadCount()).isEqualTo(1);
        assertThat(snapshotStoreB.loadCount()).isZero();
    }

    @Test
    void snapshotSourcingForATenantWithoutASnapshotFallsBackWithoutTouchingOtherTenants() {
        snapshotStoreA.store(SNAPSHOT_NAME, IDENTIFIER, snapshot("snapshot-a"), null).join();

        MessageStream<EventMessage> streamForB = sourcingEngine.source(snapshotCondition(), contextFor(TENANT_B));

        // tenant B has neither a snapshot nor events, so its stream is empty rather than snapshot-led
        assertThat(streamForB.next()).isEmpty();
        assertThat(snapshotStoreB.loadCount()).isEqualTo(1);
        assertThat(snapshotStoreA.loadCount()).isZero();
    }

    private static SourcingCondition snapshotCondition() {
        return SourcingCondition.conditionFor(new SourcingStrategy.Snapshot(SNAPSHOT_NAME, IDENTIFIER, null),
                                              EventCriteria.havingAnyTag());
    }

    private static ProcessingContext contextFor(TenantDescriptor tenant) {
        return new StubProcessingContext().withResource(TenantDescriptor.RESOURCE_KEY, tenant);
    }

    private static Snapshot snapshot(Object payload) {
        return new Snapshot(new GlobalIndexPosition(0L), "0", payload, Instant.EPOCH, Map.of());
    }

    private static Snapshot leadingSnapshot(MessageStream<EventMessage> stream) {
        EventMessage first = stream.next().orElseThrow().message();
        assertThat(first).isInstanceOf(SnapshotEventMessage.class);
        return ((SnapshotEventMessage) first).payload();
    }
}
