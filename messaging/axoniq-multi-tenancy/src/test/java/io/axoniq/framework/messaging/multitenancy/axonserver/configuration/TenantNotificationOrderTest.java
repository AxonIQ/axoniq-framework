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
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenantStreamingProcessorRestarter;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that a tenant added at runtime is streamed no matter where the {@link MultiTenantEventStorageEngine} sits
 * among the {@link TenantProvider}'s subscribers, or how long the subscribers ahead of it take.
 * <p>
 * The merged stream is assembled over the engine's tenants at the moment it opens, and re-opening it requires a
 * processor restart. That restart therefore follows the engine's own tenant changes rather than the provider's, so it
 * can only ever observe a tenant set the engine already holds. Subscriber order and subscriber lag stop being variables
 * for it, which is what these tests pin down: the provider notifies its subscribers in turn, and a subscriber ahead of
 * the engine may take real time over its own registration, such as one opening a connection for the tenant.
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
    void picksUpATenantAddedAtRuntimeWhenTheEngineHoldsTheStartupTenants() {
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
    void picksUpATenantTheEngineRegistersAfterTheProcessorsStarted() {
        buildConfiguration();
        tenantProvider.addTenant(TENANT_A);
        // Starting first, so the engine holds the tenant only after the processors opened their streams.
        configuration.start();
        tenantProvider.subscribe(routingEngine);
        publish(storeA, "A1");
        awaitHandled(TENANT_A, "A1");

        tenantProvider.addTenant(TENANT_B);
        publish(storeB, "B1");

        awaitHandled(TENANT_B, "B1");
    }

    @Test
    void picksUpATenantAddedAtRuntimeWhileASubscriberAheadOfTheEngineIsStillRegisteringIt() {
        buildConfiguration();
        tenantProvider.addTenant(TENANT_A);
        // Starting first, so anything the configuration subscribes sits ahead of the engine, and the engine is reached
        // only after a subscriber that finishes registering when released. That is the lag an asynchronously created
        // backend leaves behind, and the order in which it actually bites.
        configuration.start();
        BlockingSubscriber blockingSubscriber = new BlockingSubscriber();
        tenantProvider.subscribe(blockingSubscriber);
        tenantProvider.subscribe(routingEngine);
        publish(storeA, "A1");
        awaitHandled(TENANT_A, "A1");

        blockingSubscriber.blockRegistrations();

        // Adding on another thread, since the blocking subscriber holds up the notification that carries the tenant to
        // the engine.
        Thread tenantAddition = new Thread(() -> tenantProvider.addTenant(TENANT_B));
        tenantAddition.start();
        publish(storeB, "B1");
        blockingSubscriber.awaitReached();

        // Waiting for the stream to be serving again proves it was re-opened while the engine still lacked the tenant,
        // which is the moment a restart driven by the provider would settle on the stale set.
        try {
            publish(storeA, "A2");
            awaitHandled(TENANT_A, "A2");
            // The stream is still serving the previous tenant set. A restart driven by the provider would have re-opened
            // it here, over a set that does not yet include the new tenant.
            assertThat(routingEngine.tenants()).doesNotContain(TENANT_B);
        } finally {
            blockingSubscriber.release();
        }
        awaitTermination(tenantAddition);

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

        // The Axon Server enhancer is disabled here, so the handover it performs in production is done by hand.
        configuration.getComponent(MultiTenantStreamingProcessorRestarter.class).follow(routingEngine);
    }

    private static void awaitTermination(Thread thread) {
        try {
            thread.join(Duration.ofSeconds(10).toMillis());
            assertThat(thread.isAlive()).as("the tenant addition should have finished").isFalse();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the tenant addition to finish", interrupted);
        }
    }

    private void awaitHandled(TenantDescriptor tenant, Object payload) {
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(handled).contains(new Handled(tenant, payload)));
    }

    /**
     * A tenant-aware component that finishes registering a tenant only once it is released, standing in for one that
     * opens a connection for the tenant. Subscribed before the routing engine, so it holds up the engine's registration
     * of every later tenant.
     */
    private static class BlockingSubscriber implements MultiTenantAwareComponent {

        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicBoolean blocking = new AtomicBoolean();

        private void blockRegistrations() {
            blocking.set(true);
        }

        private void awaitReached() {
            await(reached);
        }

        private void release() {
            released.countDown();
        }

        @Override
        public Registration registerTenant(TenantDescriptor tenantDescriptor) {
            if (blocking.get()) {
                reached.countDown();
                await(released);
            }
            return () -> true;
        }

        @Override
        public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
            return registerTenant(tenantDescriptor);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("blocking", blocking.get());
            descriptor.describeProperty("released", released.getCount() == 0);
        }

        private static void await(CountDownLatch latch) {
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The blocking subscriber was never reached or released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting on the blocking subscriber", interrupted);
            }
        }
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
