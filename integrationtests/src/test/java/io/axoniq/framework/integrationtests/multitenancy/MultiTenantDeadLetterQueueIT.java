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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.deadletter.Decisions;
import io.axoniq.framework.messaging.deadletter.InMemorySequencedDeadLetterQueue;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.deadletter.TenantAwareSequencedDeadLetterQueueFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static java.util.concurrent.CompletableFuture.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for tenant-aware dead-letter queue operations against a multi-context Axon Server.
 *
 * @author Jan Galinski
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantDeadLetterQueueIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String TENANT_A = "dlq-tenant-a";
    private static final String TENANT_B = "dlq-tenant-b";
    private static final String PROCESSOR_NAME = "multi-tenant-dlq";
    private static final String COMPONENT_NAME = "failing-handler";

    private final List<String> handledTenants = new CopyOnWriteArrayList<>();
    private final List<String> retriedTenants = new CopyOnWriteArrayList<>();
    private final List<String> factoryTenants = new CopyOnWriteArrayList<>();

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);
        application = buildApplication();
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(application.getComponent(TenantProvider.class).tenants())
                       .extracting(TenantDescriptor::tenantId)
                       .contains(TENANT_A, TENANT_B));
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            StreamingEventProcessor processor = application.getComponents(StreamingEventProcessor.class)
                                                          .get(PROCESSOR_NAME);
            assertThat(processor).isNotNull();
            assertThat(processor.isRunning()).isTrue();
        });
        EventStorageEngine eventStorageEngine = application.getComponent(EventStorageEngine.class);
        assertThat(eventStorageEngine).isInstanceOf(MultiTenantEventStorageEngine.class);
        assertThat(((MultiTenantEventStorageEngine) eventStorageEngine).tenants())
                .extracting(TenantDescriptor::tenantId)
                .contains(TENANT_A, TENANT_B);
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
    void routesEnqueueAndProcessingToTheTenantInTheProcessingContext() {
        // given
        publishEvent(TENANT_A, "event-a");
        publishEvent(TENANT_B, "event-b");

        // when
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(firstStoredEvent(TENANT_A)).isNotNull();
            assertThat(firstStoredEvent(TENANT_B)).isNotNull();
        });
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(handledTenants).containsExactlyInAnyOrder(TENANT_A, TENANT_B)
        );
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(factoryTenants).containsExactlyInAnyOrder(TENANT_A, TENANT_B)
        );
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(deadLetterQueue().size(contextFor(TENANT_A)).join()).isEqualTo(1L);
            assertThat(deadLetterQueue().size(contextFor(TENANT_B)).join()).isEqualTo(1L);
        });

        deadLetterQueue().process(letter -> true, letter -> {
            retriedTenants.add(letter.context().getResource(TenantDescriptor.RESOURCE_KEY).tenantId());
            return completedFuture(Decisions.evict());
        }, contextFor(TENANT_A)).join();

        // then
        assertThat(retriedTenants).containsExactly(TENANT_A);
        assertThat(deadLetterQueue().size(contextFor(TENANT_A)).join()).isZero();
        assertThat(deadLetterQueue().size(contextFor(TENANT_B)).join()).isEqualTo(1L);
    }

    private AxonConfiguration buildApplication() {
        var processor = EventProcessorModule
                .pooledStreaming(PROCESSOR_NAME)
                .eventHandlingComponents(components -> components.declarative(COMPONENT_NAME,
                                                                               configuration -> failingComponent()))
                // Enables DLQ support for this processor. The multi-tenancy enhancer decorates its queue factory.
                .customized((configuration, pooled) -> pooled.extend(DeadLetterQueueConfiguration.class,
                                                                       () -> new DeadLetterQueueConfiguration().enabled()));

        return EventSourcingConfigurer.create()
                                      .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                      .componentRegistry(registry -> registry.registerComponent(
                                              TenantResolver.class, configuration -> new MetadataBasedTenantResolver()))
                                      .componentRegistry(registry -> registry.registerComponent(
                                              TenantConnectPredicate.class,
                                              configuration -> tenant -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT)
                                                                              .contains(tenant.tenantId())))
                                      .componentRegistry(registry -> registry.registerComponent(
                                              TenantAwareSequencedDeadLetterQueueFactory.class,
                                              configuration -> (tenant, processingGroup, queueConfiguration) -> {
                                                  // The registry creates and caches one of these queues per tenant and
                                                  // processing group. A real application selects tenant-specific storage here.
                                                  factoryTenants.add(tenant.tenantId());
                                                  return InMemorySequencedDeadLetterQueue.defaultQueue();
                                              }))
                                      // The processor itself needs no tenant-specific configuration: DLQ operations
                                      // carry their tenant in the ProcessingContext and are routed by the registry.
                                      .messaging(messaging -> messaging.eventProcessing(
                                              processing -> processing.pooledStreaming(
                                                      pooled -> pooled.processor(processor))))
                                      .start();
    }

    private SimpleEventHandlingComponent failingComponent() {
        SimpleEventHandlingComponent component = SimpleEventHandlingComponent.create(COMPONENT_NAME,
                                                                                       SequentialPolicy.INSTANCE);
        component.subscribe(new QualifiedName("test", "TenantDlqEvent"), (event, context) -> {
            handledTenants.add(TenantDescriptor.fromContext(context).orElseThrow().tenantId());
            throw new IllegalStateException("Expected failure for event " + event.identifier());
        });
        return component;
    }

    private void publishEvent(String tenantId, String id) {
        UnitOfWorkFactory unitOfWorkFactory = application.getComponent(UnitOfWorkFactory.class);
        var unitOfWork = unitOfWorkFactory.create();
        unitOfWork.runOnInvocation(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            EventAppender.forContext(context).append(new TenantDlqEvent(id));
        });
        unitOfWork.execute().orTimeout(15, TimeUnit.SECONDS).join();
    }

    private StreamEventsResponse firstStoredEvent(String tenantId) {
        AxonServerConnectionManager connectionManager = application.getComponent(AxonServerConnectionManager.class);
        try (ResultStream<StreamEventsResponse> stream = connectionManager.getConnection(tenantId)
                                                                          .dcbEventChannel()
                                                                          .stream(StreamEventsRequest.newBuilder()
                                                                                                      .setFromSequence(0)
                                                                                                      .build())) {
            return stream.nextIfAvailable(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrieving the event stored for tenant ["
                                                    + tenantId + "]", e);
        }
    }

    @SuppressWarnings("unchecked")
    private SequencedDeadLetterQueue<EventMessage> deadLetterQueue() {
        String queueName = "DeadLetterQueue[EventHandlingComponent[" + PROCESSOR_NAME + "][" + COMPONENT_NAME + "]]";
        return moduleConfiguration().getComponent(SequencedDeadLetterQueue.class, queueName);
    }

    private Configuration moduleConfiguration() {
        return application.getModuleConfiguration("EventProcessor[" + PROCESSOR_NAME + "]")
                          .or(() -> application.getModuleConfiguration(PROCESSOR_NAME))
                          .orElseThrow();
    }

    private ProcessingContext contextFor(String tenantId) {
        return new StubProcessingContext().withResource(TenantDescriptor.RESOURCE_KEY,
                                                        TenantDescriptor.tenantWithId(tenantId));
    }

    @Event(namespace = "test", name = "TenantDlqEvent", version = "1.0.0")
    record TenantDlqEvent(String id) {

    }
}
