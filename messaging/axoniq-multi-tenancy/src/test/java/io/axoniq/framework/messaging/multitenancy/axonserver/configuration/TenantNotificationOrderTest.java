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
 * Verifies that a tenant added at runtime is streamed regardless of the order in which the tenant provider notifies its
 * subscribers.
 * <p>
 * Both the {@link MultiTenantEventStorageEngine} and the streaming processor restarter follow the
 * {@link TenantProvider}, and both subscribe in the same start phase, so their relative notification order is not
 * fixed. The engine knows the current tenants and the restarter re-opens the stream, so an order that put the restarter
 * first could plausibly re-open the stream over a tenant set that does not yet include the tenant just added.
 * <p>
 * It does not: both orders below pick the tenant up. The tests are here to keep it that way, since the other tests in
 * this package all subscribe the engine before starting the configuration, which fixes it ahead of the restarter and
 * leaves the other order untested.
 */
class TenantNotificationOrderTest {

    private record Handled(TenantDescriptor tenant, Object payload) {

    }

    private final InMemoryEventStorageEngine storeA = new InMemoryEventStorageEngine();
    private final InMemoryEventStorageEngine storeB = new InMemoryEventStorageEngine();
    private final Map<TenantDescriptor, EventStorageEngine> stores = Map.of(TENANT_A, storeA, TENANT_B, storeB);
    private final StubTenantProvider tenantProvider = new StubTenantProvider();
    private final List<Handled> handled = new CopyOnWriteArrayList<>();

    private MultiTenantEventStorageEngine routingEngine;
    private AxonConfiguration configuration;

    @AfterEach
    void shutdownConfiguration() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void picksUpATenantAddedAtRuntimeWhenTheEngineIsNotifiedFirst() {
        buildConfiguration();
        tenantProvider.subscribe(routingEngine);
        tenantProvider.addTenant(TENANT_A);
        configuration.start();
        publish(storeA, "A1");
        awaitHandled(TENANT_A, "A1");

        tenantProvider.addTenant(TENANT_B);
        publish(storeB, "B1");

        awaitHandled(TENANT_B, "B1");
    }

    @Test
    void picksUpATenantAddedAtRuntimeWhenTheRestarterIsNotifiedFirst() {
        buildConfiguration();
        tenantProvider.addTenant(TENANT_A);
        // Starting subscribes the restarter, so subscribing the engine afterwards puts it second in line for every
        // later tenant change. Nothing in the contract fixes that order.
        configuration.start();
        tenantProvider.subscribe(routingEngine);
        publish(storeA, "A1");
        awaitHandled(TENANT_A, "A1");

        tenantProvider.addTenant(TENANT_B);
        publish(storeB, "B1");

        awaitHandled(TENANT_B, "B1");
    }

    private void buildConfiguration() {
        TenantEventStorageEngineFactory engineFactory = stores::get;
        TenantSnapshotStoreFactory snapshotStoreFactory = tenant -> new InMemorySnapshotStore();
        routingEngine = new MultiTenantEventStorageEngine(
                engineFactory,
                snapshotStoreFactory,
                new TenantRouter(new MetadataBasedTenantResolver(), tenantProvider));

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

    private void awaitHandled(TenantDescriptor tenant, Object payload) {
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(handled).contains(new Handled(tenant, payload)));
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
