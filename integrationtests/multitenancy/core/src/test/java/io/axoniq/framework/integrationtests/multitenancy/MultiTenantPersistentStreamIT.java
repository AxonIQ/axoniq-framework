/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.axonserver.grpc.streams.StreamStatus;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSourceFactory;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamIdResolver;
import io.axoniq.framework.messaging.multitenancy.configuration.StaticTenantConnectPredicate;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.axonframework.common.Registration;
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
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test verifying that a persistent stream is consumed from every tenant's Axon Server context, and that
 * every event reaches its handler labelled with the tenant whose stream delivered it.
 * <p>
 * A single subscribing event processor consumes one configured stream. With multi-tenancy active that stream exists in
 * each tenant's context, so the processor consumes both tenants while the handler can tell them apart. A
 * {@link MultiTenantPersistentStreamIdResolver} gives each tenant's stream an id of its own, so these flows also cover
 * streams opened under a resolved id.
 * <p>
 * The application, contexts, and stream are built once for the whole class rather than per test: the subscribing
 * processor only ever sees events published after it started, so tests stay isolated from each other without paying
 * for a fresh application per test.
 *
 * @author Jakob Hatzl
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantPersistentStreamIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String TENANT_A = "stream-tenant-a";
    private static final String TENANT_B = "stream-tenant-b";
    private static final String DYNAMIC_TENANT = "stream-tenant-dynamic";

    private static AxonServerTestInfrastructure.ContextManager contextManager;
    private static AxonConfiguration application;
    private static String streamName;
    private static final List<HandledEvent> handled = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void setUpClass() {
        INFRASTRUCTURE.start();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);
        streamName = "mt-stream-" + UUID.randomUUID();
        application = buildApplication();
    }

    @AfterAll
    static void tearDownClass() {
        if (application != null) {
            application.shutdown();
            application = null;
        }
        contextManager.deleteContexts(TENANT_A, TENANT_B, DYNAMIC_TENANT);
        INFRASTRUCTURE.stop();
    }

    @BeforeEach
    void setUp() {
        handled.clear();
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
        String addedTenant = DYNAMIC_TENANT;
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

    @Test
    void opensEveryTenantsStreamUnderTheResolvedStreamId() {
        // given the processor is running, so every tenant's stream is open
        AxonServerConnectionManager connectionManager = application.getComponent(AxonServerConnectionManager.class);

        // then Axon Server holds each tenant's stream under the resolved id, not under the configured name
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(streamIdsIn(connectionManager, TENANT_A))
                    .contains(streamName + "@" + TENANT_A)
                    .doesNotContain(streamName);
            assertThat(streamIdsIn(connectionManager, TENANT_B))
                    .contains(streamName + "@" + TENANT_B)
                    .doesNotContain(streamName);
        });
    }

    /**
     * Verifies the Spring Boot wiring: a {@link MultiTenantPersistentStreamIdResolver} declared as a bean is used for
     * the persistent stream configured under {@code axon.axonserver.persistent-streams}, which Spring Boot builds through
     * the multi-tenant {@link io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory}.
     */
    @Nested
    class SpringBoot {

        @Test
        void opensEveryTenantsStreamUnderTheIdResolvedByADeclaredBean() {
            // given a Spring Boot application declaring a resolver bean and configuring a persistent stream
            String springStreamName = "mt-spring-stream-" + UUID.randomUUID();
            try (ConfigurableApplicationContext context = startSpringApplication(springStreamName)) {
                SubscribableEventSource source = context.getBean(springStreamName, SubscribableEventSource.class);
                AxonServerConnectionManager connectionManager =
                        context.getBean(AxonConfiguration.class).getComponent(AxonServerConnectionManager.class);

                // when the configured stream gets a subscriber, which opens a stream in every tenant's context
                Registration subscription = source.subscribe(
                        (events, processingContext) -> CompletableFuture.completedFuture(null)
                );

                // then Axon Server holds each tenant's stream under the id the declared bean resolved
                try {
                    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                        assertThat(streamIdsIn(connectionManager, TENANT_A))
                                .contains(springStreamName + "@" + TENANT_A)
                                .doesNotContain(springStreamName);
                        assertThat(streamIdsIn(connectionManager, TENANT_B))
                                .contains(springStreamName + "@" + TENANT_B)
                                .doesNotContain(springStreamName);
                    });
                } finally {
                    subscription.cancel();
                }
            }
        }

        private static ConfigurableApplicationContext startSpringApplication(String streamName) {
            return new SpringApplicationBuilder(SpringStreamApplication.class)
                    .web(WebApplicationType.NONE)
                    .properties(
                            "axon.axonserver.servers=" + SharedAxonServerContainer.INSTANCE.getHost() + ":"
                                    + SharedAxonServerContainer.INSTANCE.getGrpcPort(),
                            // only this class's tenants, since other test classes share the Axon Server container
                            StaticTenantConnectPredicate.TENANTS_PROPERTY + "=" + TENANT_A + "," + TENANT_B,
                            "axon.axonserver.persistent-streams." + streamName + ".initial-segment-count=1"
                    )
                    .run();
        }

        @org.springframework.context.annotation.Configuration
        @EnableAutoConfiguration
        static class SpringStreamApplication {

            @Bean
            MultiTenantPersistentStreamIdResolver streamIdResolver() {
                return (name, tenant) -> name + "@" + tenant.tenantId();
            }
        }
    }

    // ----- helpers -------------------------------------------------------

    /**
     * Lists the ids of the persistent streams Axon Server holds in the given {@code context}.
     *
     * @param connectionManager the connection manager to reach the given {@code context} through
     * @param context           the Axon Server context to list the persistent streams of
     * @return the ids of the persistent streams in the given {@code context}
     */
    private static List<String> streamIdsIn(AxonServerConnectionManager connectionManager, String context) {
        return connectionManager.getConnection(context)
                                .eventChannel()
                                .persistentStreams()
                                .orTimeout(10, TimeUnit.SECONDS)
                                .join()
                                .stream()
                                .map(StreamStatus::getStreamId)
                                .toList();
    }

    private static AxonConfiguration buildApplication() {
        return EventSourcingConfigurer.create()
                                      .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                      .componentRegistry(registry -> TenantFixture.connectOnlyCustomTenantsPredicate(registry, "stream-tenant-"))
                                      // every tenant's stream is opened under an id of its own, so the flows below
                                      // also prove a resolved stream id is what reaches Axon Server
                                      .componentRegistry(registry -> registry.registerComponent(
                                              MultiTenantPersistentStreamIdResolver.class,
                                              c -> (name, tenant) -> name + "@" + tenant.tenantId()))
                                      .messaging(messaging -> messaging.eventProcessing(
                                              processing -> processing.subscribing(
                                                      subscribing -> subscribing.processor(buildProcessorModule()))))
                                      .start();
    }

    private static SubscribingEventProcessorModule buildProcessorModule() {
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
    private static EventHandlingComponent buildHandlingComponent() {
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

    private static SubscribableEventSource buildMultiTenantStreamSource(Configuration configuration) {
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
    private static void publishEvent(String tenantId, String id) {
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
