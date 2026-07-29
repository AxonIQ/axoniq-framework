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

package io.axoniq.framework.messaging.multitenancy.axonserver.configuration;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test wiring a real pooled streaming event processor over the tenant-routing event store, verifying that a
 * projection consumes every tenant's events, that the tenant of a streamed event reaches the handler's
 * {@code ProcessingContext} without leaking across tenants, and that a tenant added at runtime is picked up once the
 * processor restarts.
 * <p>
 * Each tenant is backed by its own {@link InMemoryEventStorageEngine}, and the {@link MultiTenantEventStorageEngine}
 * merges them behind the standard event store the processor streams from. This is the read-side counterpart to
 * {@link PerTenantEventStorageIsolationTest}, proving cross-tenant consumption end to end.
 *
 * @author Laura Devriendt
 */
class MultiTenantPooledStreamingEventProcessingTest {

    private record Handled(TenantDescriptor tenant, Object payload) {
    }

    private final InMemoryEventStorageEngine storeA = new InMemoryEventStorageEngine();
    private final InMemoryEventStorageEngine storeB = new InMemoryEventStorageEngine();
    private final Map<TenantDescriptor, EventStorageEngine> stores = Map.of(TENANT_A, storeA, TENANT_B, storeB);
    private final StubTenantProvider tenantProvider = new StubTenantProvider();
    private final List<Handled> handled = new CopyOnWriteArrayList<>();

    private AxonConfiguration configuration;

    @BeforeEach
    void buildConfiguration() {
        TenantEventStorageEngineFactory engineFactory = stores::get;
        TenantSnapshotStoreFactory snapshotStoreFactory = tenant -> new InMemorySnapshotStore();
        MultiTenantEventStorageEngine routingEngine = new MultiTenantEventStorageEngine(
                engineFactory, snapshotStoreFactory, new TenantRouter(new MetadataBasedTenantResolver(), tenantProvider));
        tenantProvider.subscribe(routingEngine);

        SimpleEventHandlingComponent projection =
                SimpleEventHandlingComponent.create("projection")
                                            .subscribe(new QualifiedName(String.class), (event, context) -> {
                                                TenantDescriptor tenant =
                                                        context.getResource(TenantDescriptor.RESOURCE_KEY);
                                                handled.add(new Handled(tenant, event.payload()));
                                                return MessageStream.empty();
                                            });

        configuration = EventSourcingConfigurer
                .create()
                .componentRegistry(registry -> {
                    MultiTenancyEnabled.enableMultiTenancyEnhancer(registry);
                    // Keep the whole flow in memory: the routing engine registered below is the streamed source.
                    registry.disableEnhancer(AxonServerConfigurationEnhancer.class)
                            .disableEnhancer(AxonServerMultiTenancyConfigurationDefaults.class)
                            .registerComponent(TenantProvider.class, config -> tenantProvider)
                            .registerComponent(EventStorageEngine.class, config -> routingEngine);
                })
                .messaging(messaging -> messaging.eventProcessing(
                        processing -> processing.pooledStreaming(
                                pooled -> pooled.defaultProcessor(
                                        "projection",
                                        components -> components.declarative("projection", config -> projection)))))
                .build();
    }

    @AfterEach
    void shutdownConfiguration() {
        configuration.shutdown();
    }

    @Test
    void handlesEachTenantsEventsAgainstItsOwnTenantWithoutLeakingAcrossTenants() {
        tenantProvider.addTenant(TENANT_A);
        tenantProvider.addTenant(TENANT_B);
        configuration.start();

        publish(storeA, "A1");
        publish(storeB, "B1");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(handled).contains(new Handled(TENANT_A, "A1"), new Handled(TENANT_B, "B1")));
        assertThat(handled).doesNotContain(new Handled(TENANT_A, "B1"), new Handled(TENANT_B, "A1"));
    }

    @Test
    void keepsHandlingAnActiveTenantWhileAnotherTenantIsIdle() {
        tenantProvider.addTenant(TENANT_A);
        tenantProvider.addTenant(TENANT_B);
        configuration.start();
        publish(storeA, "A1");
        publish(storeB, "B1");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(handled).contains(new Handled(TENANT_A, "A1"), new Handled(TENANT_B, "B1")));

        // only tenant A produces a further event while tenant B stays idle
        publish(storeA, "A2");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(handled).contains(new Handled(TENANT_A, "A2")));
    }

    @Test
    void picksUpATenantAddedAtRuntime() {
        tenantProvider.addTenant(TENANT_A);
        configuration.start();
        publish(storeA, "A1");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(handled).contains(new Handled(TENANT_A, "A1")));

        // a second tenant is added at runtime, so the restarter re-opens the stream with it
        tenantProvider.addTenant(TENANT_B);
        publish(storeB, "B1");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handled).contains(new Handled(TENANT_B, "B1")));
    }

    /**
     * A change to the tenant set re-opens the merged stream, which must leave every other tenant exactly where it was.
     * Per tenant the processor has to behave as a single-tenant processor over that tenant's own store: resume from its
     * stored position, and deliver each of its events once.
     */
    @Nested
    class TenantSetChanges {

        @Test
        void doesNotReprocessAnExistingTenantWhenAnotherTenantIsAdded() {
            tenantProvider.addTenant(TENANT_A);
            configuration.start();
            publish(storeA, "A1");
            publish(storeA, "A2");
            awaitHandled(TENANT_A, "A2");

            tenantProvider.addTenant(TENANT_B);
            publish(storeB, "B1");
            awaitHandled(TENANT_B, "B1");

            // tenant A was already up to date, so re-opening the stream must not hand its events over a second time
            assertThat(timesHandled(TENANT_A, "A1")).isEqualTo(1);
            assertThat(timesHandled(TENANT_A, "A2")).isEqualTo(1);
        }

        @Disabled("""
                Records the duplicate delivery a tenant change can still cause. The processor recognizes an \
                already-handled event by asking whether the token it last delivered covers the token on the streamed \
                event, which is one answer for the whole token where the question is per tenant. A stored token \
                written before tenant B existed cannot cover B's position, so once that position enters the emitted \
                tokens, tenant A's events stop being recognized. Closing this needs a per-source duplicate check or \
                per-tenant processors, so until then handlers have to be idempotent. See the ADR 002 addendum.""")
        @Test
        void doesNotReprocessAnExistingTenantWhenAnAddedTenantsEventsInterleaveInTime() {
            tenantProvider.addTenant(TENANT_A);
            configuration.start();
            publish(storeA, "A1");
            awaitHandled(TENANT_A, "A1");

            // tenant B's store gets an event while B is not a tenant yet, so it is timestamped between A's two
            publish(storeB, "B1");
            publish(storeA, "A2");
            awaitHandled(TENANT_A, "A2");

            tenantProvider.addTenant(TENANT_B);
            awaitHandled(TENANT_B, "B1");

            // re-reading the merged stream now walks A1, B1, A2 in timestamp order, so tenant B's position enters
            // the emitted token before A's last event is reached. A must still not be handed A2 twice.
            assertThat(timesHandled(TENANT_A, "A1")).isEqualTo(1);
            assertThat(timesHandled(TENANT_A, "A2")).isEqualTo(1);
        }

        @Test
        void doesNotReprocessARemainingTenantWhenAnotherTenantIsRemoved() {
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);
            configuration.start();
            publish(storeA, "A1");
            publish(storeA, "A2");
            publish(storeB, "B1");
            awaitHandled(TENANT_A, "A2");
            awaitHandled(TENANT_B, "B1");

            tenantProvider.removeTenant(TENANT_B);
            publish(storeA, "A3");
            awaitHandled(TENANT_A, "A3");

            // removing tenant B says nothing about tenant A's position, so A must not replay
            assertThat(timesHandled(TENANT_A, "A1")).isEqualTo(1);
            assertThat(timesHandled(TENANT_A, "A2")).isEqualTo(1);
        }

        @Test
        void deliversEventsOfATenantWhoseStoreWasStillEmptyWhenTheStreamReopened() {
            tenantProvider.addTenant(TENANT_A);
            configuration.start();
            publish(storeA, "A1");
            awaitHandled(TENANT_A, "A1");

            // tenant B joins with an empty store. Handling a further event of tenant A shows the stream has
            // re-opened, and it re-opened while tenant B still had nothing to read.
            tenantProvider.addTenant(TENANT_B);
            publish(storeA, "A2");
            awaitHandled(TENANT_A, "A2");

            // only now does tenant B get its first event, so it arrives on an already-open stream
            publish(storeB, "B1");

            awaitHandled(TENANT_B, "B1");
        }
    }

    private void awaitHandled(TenantDescriptor tenant, Object payload) {
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(handled).contains(new Handled(tenant, payload)));
    }

    private long timesHandled(TenantDescriptor tenant, Object payload) {
        return handled.stream().filter(entry -> entry.equals(new Handled(tenant, payload))).count();
    }

    private void publish(EventStorageEngine store, String payload) {
        store.appendEvents(AppendCondition.none(),
                           null,
                           List.of(new GenericTaggedEventMessage<>(EventTestUtils.asEventMessage(payload), Set.of())))
             .join()
             .commit()
             .join();
    }
}
