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

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSourceFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test verifying that a persistent stream is consumed from every tenant's Axon Server context, and that
 * every event reaches its handler labelled with the tenant whose stream delivered it.
 * <p>
 * A single subscribing event processor consumes one configured stream. With multi-tenancy active that stream exists in
 * each tenant's context, so the processor consumes both tenants while the handler can tell them apart.
 *
 * @author Jakob Hatzl
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantPersistentStreamIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String TENANT_A = "tenant-A";
    private static final String TENANT_B = "tenant-B";

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;
    private String streamName;
    private final List<HandledEvent> handled = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);
        // A unique name per test keeps the server-side streams of one run from being joined by the next.
        streamName = "mt-stream-" + UUID.randomUUID();
        handled.clear();
        application = buildApplication();
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
            application = null;
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void consumesTheStreamOfEveryTenantLabellingEachEventWithItsOwnTenant() {
        // when an event is appended to each tenant's event store
        publishEvent(TENANT_A, "for-a");
        publishEvent(TENANT_B, "for-b");

        // then both arrive, each labelled with the tenant whose stream delivered it
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(handled).hasSize(2));
        assertThat(handled).containsExactlyInAnyOrder(new HandledEvent(TENANT_A, "for-a"),
                                                      new HandledEvent(TENANT_B, "for-b"));
    }

    @Test
    void doesNotLeakEventsOfOneTenantIntoAnother() {
        // when only one tenant has events
        publishEvent(TENANT_A, "first");
        publishEvent(TENANT_A, "second");

        // then they are all labelled with that tenant, and none is attributed to the other
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(handled).hasSize(2));
        assertThat(handled).extracting(HandledEvent::tenantId).containsOnly(TENANT_A);
    }

    @Test
    void consumesTheStreamOfATenantAddedWhileRunning() {
        // given the processor is running and consuming the tenants discovered at startup
        publishEvent(TENANT_A, "before");
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(handled).hasSize(1));

        // when a tenant is added at runtime and the provider has picked it up
        String addedTenant = "tenant-C";
        contextManager.createContext(addedTenant);
        TenantProvider tenantProvider = application.getComponent(TenantProvider.class);
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(tenantProvider.tenants())
                       .extracting(TenantDescriptor::tenantId)
                       .contains(addedTenant));

        // then its stream is opened and consumed too, while the tenant consumed before is untouched
        publishEvent(addedTenant, "after");
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(handled).contains(new HandledEvent(addedTenant, "after")));
        assertThat(handled).contains(new HandledEvent(TENANT_A, "before"));
    }

    // ----- helpers -------------------------------------------------------

    private AxonConfiguration buildApplication() {
        return EventSourcingConfigurer.create()
                                      .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                      .componentRegistry(cr -> cr.registerComponent(
                                              TenantConnectPredicate.class,
                                              c -> d -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT).contains(d.tenantId())))
                                      .messaging(messaging -> messaging.eventProcessing(
                                              processing -> processing.subscribing(
                                                      subscribing -> subscribing.processor(buildProcessorModule()))))
                                      .start();
    }

    private SubscribingEventProcessorModule buildProcessorModule() {
        return EventProcessorModule
                .subscribing("multi-tenant-persistent-stream-module")
                .eventHandlingComponents(components -> components.declarative(
                        "multi-tenant-persistent-stream-handling-component",
                        configuration -> buildHandlingComponent()
                ))
                .customized((configuration, subscribing) -> subscribing.eventSource(
                        buildMultiTenantStreamSource(configuration)
                ));
    }

    /**
     * Records the tenant on the processing context together with the handled event, which is what proves the labelling:
     * the tenant can only have come from the stream that delivered the event, since no interceptor puts a tenant on the
     * context of an event.
     *
     * @return the handling component recording every event it handles
     */
    private EventHandlingComponent buildHandlingComponent() {
        SimpleEventHandlingComponent handlingComponent =
                SimpleEventHandlingComponent.create("multi-tenant-persistent-stream-handling",
                                                    SequentialPolicy.INSTANCE);
        handlingComponent.subscribe(
                new QualifiedName("test", "TenantStreamTestEvent"),
                (event, context) -> {
                    TenantDescriptor tenant = TenantDescriptor.fromContext(context).orElseThrow();
                    handled.add(new HandledEvent(tenant == null ? null : tenant.tenantId(),
                                                 event.payloadAs(TenantStreamTestEvent.class).id()));
                    return MessageStream.empty();
                }
        );
        return handlingComponent;
    }

    private SubscribableEventSource buildMultiTenantStreamSource(Configuration configuration) {
        int segmentCount = 1;
        PersistentStreamProperties properties = new PersistentStreamProperties(
                streamName,
                segmentCount,
                PersistentStreamSequencingPolicy.SEQUENTIAL_POLICY,
                Collections.emptyList(),
                "TAIL",
                null  // no server-side filter
        );
        // A factory rather than one scheduler, so every tenant's stream runs on threads of its own, named after the
        // tenant it serves. The source shuts a tenant's pool down when that tenant's stream closes.
        return new MultiTenantPersistentStreamEventSourceFactory()
                .build(streamName,
                       properties,
                       poolName -> PersistentStreamScheduledExecutorBuilder.defaultFactory()
                                                                          .build(segmentCount, poolName),
                       10,
                       configuration);
    }

    /**
     * Appends an event to the event store of the given {@code tenantId}.
     * <p>
     * The tenant is put on the processing context the append runs in, which is how the routing event storage engine
     * picks a tenant for an append made inside a transaction, and what the tenant-descriptor handler interceptor does on
     * the command path.
     *
     * @param tenantId the tenant whose event store receives the event
     * @param id       the identifier of the published event
     */
    private void publishEvent(String tenantId, String id) {
        UnitOfWorkFactory unitOfWorkFactory = application.getComponent(UnitOfWorkFactory.class);
        var unitOfWork = unitOfWorkFactory.create();
        unitOfWork.runOnInvocation(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            EventAppender.forContext(context).append(new TenantStreamTestEvent(id));
        });
        unitOfWork.execute().orTimeout(15, TimeUnit.SECONDS).join();
    }

    /**
     * One handled event, pairing the tenant it was labelled with and the event's identifier.
     *
     * @param tenantId the identifier of the tenant found on the processing context, or {@code null} when absent
     * @param id       the identifier of the handled event
     */
    private record HandledEvent(String tenantId, String id) {

    }

    /**
     * Event payload published into a tenant's event store and consumed from that tenant's persistent stream.
     *
     * @param id a human-readable event identifier
     */
    @Event(namespace = "test", name = "TenantStreamTestEvent", version = "1.0.0")
    record TenantStreamTestEvent(String id) {

    }
}
