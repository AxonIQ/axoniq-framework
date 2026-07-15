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
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.eventsourcing.snapshot.store.tracing.TracingSnapshotStore;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.EventSourcedEntityFactory;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
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
 * Integration test for snapshot-store tracing through the real configuration: an
 * {@link EventSourcingConfigurer} wires an event-sourced entity with a {@link SnapshotPolicy}, the
 * ServiceLoader-discovered {@code EventSourcingTracingConfigurationEnhancer} decorates the {@link SnapshotStore}
 * slot, and the snapshot written after the policy triggers must produce the {@code SnapshotStore.store} span — while
 * a subsequent entity load consuming that snapshot must produce the {@code SnapshotStore.load} span.
 */
class SnapshotStoreTracingConfigurationIntegrationTest {

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
    void snapshotCreationAndConsumptionProduceTheStoreAndLoadSpans() {
        // given a configured application whose entity snapshots after every event
        configuration = startApplication();
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

        // when enough commands hit the same entity to trigger the snapshot policy
        sendAddGuest(commandGateway, "alice");
        sendAddGuest(commandGateway, "bob");

        // then the (asynchronously written) snapshot produces the store span
        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertThat(spanNames())
                       .anyMatch(name -> name.startsWith(TracingSnapshotStore.STORE_SPAN)));
        SpanData storeSpan = spanStartingWith(TracingSnapshotStore.STORE_SPAN);
        assertThat(storeSpan.getKind()).isEqualTo(SpanKind.INTERNAL);

        // and when the next command loads the entity from that snapshot, the load span appears
        sendAddGuest(commandGateway, "carol");
        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertThat(spanNames())
                       .anyMatch(name -> name.startsWith(TracingSnapshotStore.LOAD_SPAN)));
        SpanData loadSpan = spanStartingWith(TracingSnapshotStore.LOAD_SPAN);
        assertThat(loadSpan.getKind()).isEqualTo(SpanKind.INTERNAL);
    }

    private void sendAddGuest(CommandGateway commandGateway, String guest) {
        commandGateway.send(new AddGuest("list-1", guest))
                      .resultAs(String.class)
                      .orTimeout(30, TimeUnit.SECONDS)
                      .join();
    }

    private AxonConfiguration startApplication() {
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
                                        // afterEvents(n) triggers when eventsApplied() > n — with 0, any sourcing
                                        // that applied at least one event snapshots the entity.
                                        .snapshotPolicy(SnapshotPolicy.afterEvents(0))
                                        .build();
        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("tracing-snapshot-test")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new AddGuestHandler())
                                     .build();
        return EventSourcingConfigurer
                .create()
                .registerEntity(guestListEntity)
                .messaging(messaging -> messaging.registerCommandHandlingModule(() -> commandHandlingModule))
                .componentRegistry(registry -> registry
                        // Stay local: the Axon Server connector would otherwise take over the buses and the
                        // event store.
                        .disableEnhancer(AxonServerConfigurationEnhancer.class)
                        .registerComponent(SnapshotStore.class, c -> new InMemorySnapshotStore())
                        .registerComponent(SpanFactory.class, c -> spanFactory)
                        .registerComponent(Tracer.class, c -> tracing.tracer())
                )
                .start();
    }

    private List<String> spanNames() {
        return spanExporter.getFinishedSpanItems().stream().map(SpanData::getName).toList();
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
