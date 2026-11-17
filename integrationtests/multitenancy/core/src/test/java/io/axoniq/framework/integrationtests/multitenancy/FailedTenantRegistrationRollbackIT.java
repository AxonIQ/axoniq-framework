/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSourceFactory;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamIdResolver;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.commandhandling.gateway.CommandResult;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Integration test verifying that a tenant whose registration fails while the application runs is rolled back instead
 * of being left half-registered.
 * <p>
 * A persistent stream processor consumes every tenant's stream, and a user-supplied
 * {@link MultiTenantPersistentStreamIdResolver} throws for one tenant. When that tenant's context is created in Axon
 * Server at runtime, the registration of the stream source fails, so the provider must withdraw the tenant again,
 * while the tenant that works keeps handling commands and streaming events.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
@Timeout(120)
class FailedTenantRegistrationRollbackIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String TENANT_PREFIX = "rollback-tenant-";
    private static final String HEALTHY_TENANT = TENANT_PREFIX + "healthy";
    private static final String FAILING_TENANT = TENANT_PREFIX + "failing";

    private static AxonServerTestInfrastructure.ContextManager contextManager;
    private static AxonConfiguration application;
    private static final AtomicInteger failingResolutions = new AtomicInteger();
    private static final List<HandledEvent> handled = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void setUpClass() {
        INFRASTRUCTURE.start();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(HEALTHY_TENANT);
        application = buildApplication("rollback-stream-" + UUID.randomUUID());
    }

    @AfterAll
    static void tearDownClass() {
        if (application != null) {
            application.shutdown();
            application = null;
        }
        contextManager.deleteContexts(HEALTHY_TENANT, FAILING_TENANT);
        INFRASTRUCTURE.stop();
    }

    @Test
    void aTenantWhoseStreamCannotBeOpenedAtRuntimeIsRolledBackAndTheHealthyTenantKeepsWorking() {
        // given an application running with one healthy tenant
        TenantProvider tenantProvider = application.getComponent(TenantProvider.class);
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(tenantProvider.tenants())
                       .extracting(TenantDescriptor::tenantId)
                       .contains(HEALTHY_TENANT));

        // when the context of the tenant whose stream id cannot be resolved is created while the app runs
        contextManager.createContext(FAILING_TENANT);

        // then the provider handled the creation update (the resolver was asked for the tenant) and withdrew it
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(failingResolutions.get()).isPositive());
        TenantDescriptor failing = TenantDescriptor.tenantWithId(FAILING_TENANT);
        await().atMost(30, TimeUnit.SECONDS)
               .during(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   assertThat(tenantProvider.isKnown(failing)).isFalse();
                   assertThat(tenantProvider.tenants()).extracting(TenantDescriptor::tenantId)
                                                       .doesNotContain(FAILING_TENANT);
               });

        // and a command for it is rejected as an unknown tenant
        CommandGateway commandGateway = application.getComponent(CommandGateway.class);
        CommandResult rejected = commandGateway.send(new AppendEventCommand("for-failing"),
                                                     tenantMetadata(FAILING_TENANT),
                                                     null);
        assertThatThrownBy(() -> rejected.getResultMessage().orTimeout(15, TimeUnit.SECONDS).join())
                .hasCauseInstanceOf(TenantNotResolvedException.class);

        // and the healthy tenant handles a command whose event reaches its persistent stream handler
        commandGateway.send(new AppendEventCommand("for-healthy"), tenantMetadata(HEALTHY_TENANT), null)
                      .getResultMessage()
                      .orTimeout(15, TimeUnit.SECONDS)
                      .join();
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(handled).containsExactly(
                       new HandledEvent(HEALTHY_TENANT, "for-healthy")));
    }

    private static Metadata tenantMetadata(String tenantId) {
        return Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, tenantId);
    }

    private static AxonConfiguration buildApplication(String streamName) {
        CommandHandlingModule.CommandHandlerPhase commandHandlingModule =
                CommandHandlingModule.named("rollback-it-commands")
                                     .commandHandlers()
                                     .commandHandler(new QualifiedName(AppendEventCommand.class),
                                                     FailedTenantRegistrationRollbackIT::appendEvent);
        return EventSourcingConfigurer.create()
                                      .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                      .componentRegistry(registry -> TenantFixture.connectOnlyCustomTenantsPredicate(
                                              registry, TENANT_PREFIX))
                                      .componentRegistry(registry -> registry.registerComponent(
                                              MultiTenantPersistentStreamIdResolver.class,
                                              c -> (name, tenant) -> {
                                                  if (FAILING_TENANT.equals(tenant.tenantId())) {
                                                      failingResolutions.incrementAndGet();
                                                      throw new IllegalStateException(
                                                              "cannot resolve a stream id for " + tenant.tenantId());
                                                  }
                                                  return name;
                                              }))
                                      .componentRegistry(registry -> registry.registerModule(
                                              commandHandlingModule.build()))
                                      .messaging(messaging -> messaging.eventProcessing(
                                              processing -> processing.subscribing(
                                                      subscribing -> subscribing.processor(
                                                              buildProcessorModule(streamName)))))
                                      .start();
    }

    private static MessageStream.Single<CommandResultMessage> appendEvent(CommandMessage command,
                                                                          ProcessingContext context) {
        EventAppender.forContext(context)
                     .append(new RollbackTestEvent(command.payloadAs(AppendEventCommand.class).id()));
        return MessageStream.just(new GenericCommandResultMessage(new MessageType(String.class), "ok"));
    }

    private static SubscribingEventProcessorModule buildProcessorModule(String streamName) {
        return EventProcessorModule
                .subscribing("rollback-it-processor")
                .eventHandlingComponents(components -> components.declarative(
                        "rollback-it-handling-component",
                        configuration -> buildHandlingComponent()
                ))
                .customized((configuration, subscribing) -> subscribing.eventSource(
                        buildStreamSource(streamName, configuration)
                ));
    }

    private static EventHandlingComponent buildHandlingComponent() {
        SimpleEventHandlingComponent handlingComponent =
                SimpleEventHandlingComponent.create("rollback-it-handling", SequentialPolicy.INSTANCE);
        handlingComponent.subscribe(
                new QualifiedName("test", "RollbackTestEvent"),
                (event, context) -> {
                    TenantDescriptor tenant = TenantDescriptor.fromContext(context).orElseThrow();
                    handled.add(new HandledEvent(tenant.tenantId(), event.payloadAs(RollbackTestEvent.class).id()));
                    return MessageStream.empty();
                }
        );
        return handlingComponent;
    }

    private static SubscribableEventSource buildStreamSource(String streamName, Configuration configuration) {
        int segmentCount = 1;
        PersistentStreamProperties properties = new PersistentStreamProperties(
                streamName,
                segmentCount,
                PersistentStreamSequencingPolicy.SEQUENTIAL_POLICY,
                Collections.emptyList(),
                "TAIL",
                null
        );
        return new MultiTenantPersistentStreamEventSourceFactory()
                .build(streamName,
                       properties,
                       poolName -> PersistentStreamScheduledExecutorBuilder.defaultFactory()
                                                                          .build(segmentCount, poolName),
                       10,
                       configuration);
    }

    private record HandledEvent(String tenantId, String id) {

    }

    record AppendEventCommand(String id) {

    }

    @Event(namespace = "test", name = "RollbackTestEvent", version = "1.0.0")
    record RollbackTestEvent(String id) {

    }
}
