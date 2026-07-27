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
import io.axoniq.framework.messaging.multitenancy.api.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
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
 * Proves that snapshotting works per tenant when it goes through the standard event-sourcing wiring rather than the
 * routing components directly.
 * <p>
 * The routing engine is registered as both the {@link EventStorageEngine} and the {@link SnapshotStore}, so the
 * event-sourcing defaults leave it alone instead of complementing it with a {@link SnapshotCapableEventStorageEngine}
 * above the fan-out. Snapshot resolution therefore happens below the routing engine, in each tenant's own snapshot
 * capable engine. A snapshot stored for one tenant is used only when sourcing that tenant, and a tenant without one
 * falls back to a snapshot-free stream without ever touching another tenant's store.
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
    private MultiTenantEventStorageEngine routingEngine;
    private EventStorageEngine sourcingEngine;

    @BeforeEach
    void setUp() {
        // Per-tenant engines that are not their own snapshot store, complemented while they are built, exactly as
        // AxonServerTenantEventStorageEngineFactory does for a real Axon Server engine.
        tenantEngines.entry(TENANT_A, complementedEngineFor(TENANT_A));
        tenantEngines.entry(TENANT_B, complementedEngineFor(TENANT_B));
        MetadataBasedTenantResolver tenantResolver = new MetadataBasedTenantResolver();
        routingEngine = new MultiTenantEventStorageEngine(
                tenantEngines::apply, tenantSnapshotStores::apply, tenantResolver, tenantEngines);

        StubTenantProvider tenantProvider = new StubTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        tenantProvider.addTenant(TENANT_B);

        configuration = EventSourcingConfigurer
                .create()
                .componentRegistry(registry -> {
                    MultiTenancyEnabled.enableMultiTenancyEnhancer(registry);
                    // Register the routing engine under both types as the Axon Server defaults do, but backed by
                    // recording per-tenant doubles and without reaching a real Axon Server.
                    registry.disableEnhancer(AxonServerConfigurationEnhancer.class)
                            .disableEnhancer(AxonServerMultiTenancyConfigurationDefaults.class)
                            .registerComponent(TenantProvider.class, config -> tenantProvider)
                            .registerComponent(EventStorageEngine.class, config -> routingEngine)
                            .registerComponent(SnapshotStore.class, config -> routingEngine);
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

    private EventStorageEngine complementedEngineFor(TenantDescriptor tenant) {
        return TenantEventStorageEngineFactory.snapshotCapable(new RecordingEventStorageEngine(),
                                                               tenantSnapshotStores.apply(tenant));
    }

    @Test
    void theRoutingEngineIsRegisteredUndecorated() {
        // being the snapshot store as well keeps the framework from complementing the routing engine, which would
        // resolve the snapshot before a tenant is known
        assertThat(sourcingEngine).isSameAs(routingEngine)
                                  .isNotInstanceOf(SnapshotCapableEventStorageEngine.class);
    }

    @Test
    void snapshotsAreStoredInAndLoadedFromTheSourcedTenantsOwnStore() {
        // the store side travels through the registered SnapshotStore, which is the routing engine itself
        SnapshotStore snapshotStore = configuration.getComponent(SnapshotStore.class);
        Snapshot snapshotA = snapshot("snapshot-a");

        snapshotStore.store(SNAPSHOT_NAME, IDENTIFIER, snapshotA, contextFor(TENANT_A)).join();

        assertThat(snapshotStoreA.storeCount()).isEqualTo(1);
        assertThat(snapshotStoreB.storeCount()).isZero();
        assertThat(leadingSnapshot(sourcingEngine.source(snapshotCondition(), contextFor(TENANT_A))))
                .isEqualTo(snapshotA);
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
