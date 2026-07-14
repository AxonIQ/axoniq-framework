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

package io.axoniq.framework.tracing.integration;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.tracing.micrometer.MicrometerTracingConfigurationEnhancer;
import io.axoniq.framework.tracing.micrometer.threadlocal.MicrometerThreadLocalContextPropagationConfigurationEnhancer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.modelling.tracing.TracingStateManager;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.EventSourcedEntityFactory;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.modelling.annotation.InjectEntity;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for modelling tracing through the real configuration: an
 * {@link EventSourcingConfigurer} wires an event-sourced entity, the ServiceLoader-discovered
 * {@code ModellingTracingConfigurationEnhancer} decorates the {@code StateManager} / {@code Repository} slots, and a
 * command handled with {@code @InjectEntity} must produce the {@code StateManager.loadManagedEntity <EntityType>}
 * (and, when the entity repository slot is decorated, {@code Repository.load <EntityType>}) spans inside the command
 * trace. With no {@link SpanFactory} registered, the same flow produces no spans.
 */
class RepositoryTracingConfigurationIntegrationTest {

    private static final String STATE_MANAGER_SPAN = TracingStateManager.LOAD_MANAGED_ENTITY_SPAN + " GuestList";

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private SpanFactory spanFactory;
    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        tracing = MicrometerTracingTestSetup.create();
        spanExporter = tracing.spanExporter();
        spanFactory = tracing.spanFactory();
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        tracing.close();
    }

    @Test
    void aCommandLoadingAnEventSourcedEntityProducesTheStateManagerSpanInsideTheCommandTrace() {
        // given a configured application with an event-sourced entity and the OTel SpanFactory registered
        configuration = startApplication(/* registerSpanFactory */ true);
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

        // when two commands target the same entity — the second load replays the first command's event
        String result1 = commandGateway.send(new AddGuest("list-1", "alice"))
                                       .resultAs(String.class)
                                       .orTimeout(30, TimeUnit.SECONDS)
                                       .join();
        String result2 = commandGateway.send(new AddGuest("list-1", "bob"))
                                       .resultAs(String.class)
                                       .orTimeout(30, TimeUnit.SECONDS)
                                       .join();

        // then both commands saw the (re)played state and the entity-load span appears inside the command trace
        assertThat(result1).isEqualTo("guests=1");
        assertThat(result2).isEqualTo("guests=2");
        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertThat(spanNames()).contains(STATE_MANAGER_SPAN));

        SpanData stateManagerSpan = spanNamed(STATE_MANAGER_SPAN);
        SpanData dispatchSpan = spanStartingWith("CommandBus.dispatchCommand");
        assertThat(stateManagerSpan.getKind()).isEqualTo(SpanKind.INTERNAL);
        assertThat(stateManagerSpan.getTraceId()).isEqualTo(dispatchSpan.getTraceId());

        // The entity-module repository is registered through the (traced) StateManager, which wraps it in
        // TracingRepository — @InjectEntity resolves entities with a factory via loadOrCreate.
        SpanData repositorySpan = spanNamed("Repository.loadOrCreate GuestList");
        assertThat(repositorySpan.getKind()).isEqualTo(SpanKind.INTERNAL);
        assertThat(repositorySpan.getTraceId()).isEqualTo(dispatchSpan.getTraceId());
    }

    @Test
    void withoutAConfiguredSpanFactoryNoTracingSpansAreProduced() {
        // given the same configuration but WITHOUT a SpanFactory registered
        configuration = startApplication(/* registerSpanFactory */ false);
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

        // when
        String result = commandGateway.send(new AddGuest("list-1", "alice"))
                                      .resultAs(String.class)
                                      .orTimeout(30, TimeUnit.SECONDS)
                                      .join();

        // then the command is handled normally, but the InMemorySpanExporter has nothing — tracing is fully off
        assertThat(result).isEqualTo("guests=1");
        assertThat(spanExporter.getFinishedSpanItems()).isEmpty();
    }

    private AxonConfiguration startApplication(boolean registerSpanFactory) {
        EventSourcedEntityModule<String, GuestList> guestListEntity =
                EventSourcedEntityModule.declarative(String.class, GuestList.class)
                                        .messagingModel((c, model) -> model
                                                .entityEvolver((guestList, event, context) -> {
                                                    guestList.guests.add(
                                                            event.payloadAs(GuestAdded.class).guest());
                                                    return guestList;
                                                })
                                                .build())
                                        .entityFactory(c -> EventSourcedEntityFactory.fromIdentifier(GuestList::new))
                                        .criteriaResolver(c -> (listId, context) ->
                                                EventCriteria.havingTags(Tag.of("GuestList", listId)))
                                        .build();
        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("tracing-repository-test")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new AddGuestHandler())
                                     .build();
        EventSourcingConfigurer configurer = EventSourcingConfigurer
                .create()
                .registerEntity(guestListEntity)
                .messaging(messaging -> messaging.registerCommandHandlingModule(() -> commandHandlingModule))
                .componentRegistry(registry -> registry
                        // Stay local: the Axon Server connector would otherwise take over the buses and the
                        // event store.
                        .disableEnhancer(AxonServerConfigurationEnhancer.class)
                );
        if (registerSpanFactory) {
            configurer.componentRegistry(registry -> registry
                    .registerComponent(SpanFactory.class, c -> spanFactory)
                    .registerComponent(Tracer.class, c -> tracing.tracer()));
        } else {
            // No bridge components available in this scenario -- disable the Micrometer enhancers so they don't
            // fail requiring a Tracer to build their own SpanFactory / thread-local bridge.
            configurer.componentRegistry(registry -> registry
                    .disableEnhancer(MicrometerTracingConfigurationEnhancer.class)
                    .disableEnhancer(MicrometerThreadLocalContextPropagationConfigurationEnhancer.class));
        }
        return configurer.start();
    }

    private List<String> spanNames() {
        return spanExporter.getFinishedSpanItems().stream().map(SpanData::getName).toList();
    }

    private SpanData spanNamed(String name) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().equals(name))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError("No span named " + name + " in " + spanNames()));
    }

    private SpanData spanStartingWith(String prefix) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().startsWith(prefix))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError(
                                   "No span starting with " + prefix + " in " + spanNames()));
    }

    record AddGuest(@TargetEntityId String listId, String guest) {
    }

    record GuestAdded(@EventTag(key = "GuestList") String listId, String guest) {
    }

    static class GuestList {

        private final String id;
        private final List<String> guests = new ArrayList<>();

        GuestList(String id) {
            this.id = id;
        }
    }

    @SuppressWarnings("unused")
    static class AddGuestHandler {

        @CommandHandler
        public String handle(AddGuest command, @InjectEntity GuestList guestList, EventAppender eventAppender) {
            // The appended event evolves the loaded entity immediately, so the size already includes this guest.
            eventAppender.append(new GuestAdded(command.listId(), command.guest()));
            return "guests=" + guestList.guests.size();
        }
    }
}
