/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.StepWaitInfo;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWaitAssociations;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.modelling.repository.ManagedEntity;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Snapshots of the open-wait list, loaded through its repository.
 *
 * @author Stefan Dragisic
 */
class WaitAssociationsRepositoryIntegrationTest extends AbstractEventSourcedEntityRepositoryTestBase {

    private static final String MODULE = "wait-associations-module";
    private static final VersionedType DEFINITION_ID = VersionedType.of(new QualifiedName("OrderWorkflow"), "0.0.1");
    private static final Instant STARTED_AT = Instant.parse("2026-07-08T10:00:00Z");
    private static final TypeReference<Map<String, List<Map<String, Object>>>> FIELDS = new TypeReference<>() {
    };

    static Stream<Arguments> converters() {
        return Stream.of(Arguments.of(Named.of("JacksonConverter", new JacksonConverter())),
                         Arguments.of(Named.of("Jackson2Converter", new Jackson2Converter())),
                         Arguments.of(Named.of("JacksonConverter reading fields", fieldReadingConverter())));
    }

    @ParameterizedTest
    @MethodSource("converters")
    void repositoryRestoresOpenWaitsFromSnapshot(Converter converter) {
        // given
        configuration = configuration(MODULE, configurer -> configurer
                .componentRegistry(cr -> registerSnapshotStore(cr, converter))
                .snapshots(snapshots -> snapshots.enabled(true).waitAssociations(SnapshotPolicy.afterEvents(1))));
        configuration.start();
        publishTwoWaitingWorkflows();

        // when - sourcing more events than the policy allows stores a snapshot
        var sourced = load();

        // then - the open waits survive the converter that writes and reads the snapshot, the one without an event
        // name included. The stored snapshot itself may be an older one: a segment claim of the started engine
        // sources the list as well.
        assertThat(storedSnapshot()).isNotNull();
        assertThat(sourced.waitAssociations()).containsExactly(
                new WaitAssociation("wf-1", "awaitPayment", new QualifiedName("PaymentReceived"),
                                    Set.of("payload:orderId=wf-1")),
                new WaitAssociation("wf-2", "awaitUnreadable", null, Set.of())
        );
        byte[] written = converter.convert(sourced, byte[].class);
        EventSourcedWaitAssociations read = converter.convert(written, EventSourcedWaitAssociations.class);
        assertThat(read.waitAssociations()).containsExactlyElementsOf(sourced.waitAssociations());

        // and - a later load continues from the snapshot
        publish(EventMessageUtils.completedWorkflow(workflowExecutionOperations("wf-1"),
                                                    "OrderWorkflow",
                                                    DEFINITION_ID,
                                                    DefaultEventNameCustomizer.Builder.defaults()));
        assertThat(load().waitAssociations()).extracting(WaitAssociation::workflowId).containsExactly("wf-2");
    }

    @Test
    void repositoryStoresNoSnapshotWhenSnapshotsAreNotEnabled() {
        // given
        configuration = configuration(
                MODULE,
                configurer -> configurer.componentRegistry(cr -> registerSnapshotStore(cr, new JacksonConverter()))
        );
        configuration.start();
        publishTwoWaitingWorkflows();

        // when
        var openWaits = load();

        // then
        assertThat(openWaits.waitAssociations()).hasSize(2);
        assertThat(storedSnapshot()).isNull();
    }

    @ParameterizedTest
    @MethodSource("converters")
    void convertedSnapshotHoldsOnlyTheWaits(Converter converter) {
        // given
        configuration = configuration(MODULE);
        configuration.start();
        publishTwoWaitingWorkflows();
        var openWaits = load();

        // when
        byte[] stored = converter.convert(openWaits, byte[].class);

        // then - no other getter of the entity or of a wait ends up in the snapshot
        Map<String, List<Map<String, Object>>> fields = converter.convert(stored, FIELDS.getType());
        assertThat(fields).containsOnlyKeys("waits");
        assertThat(fields.get("waits")).hasSize(2);
        assertThat(fields.get("waits").getFirst())
                .containsOnlyKeys("workflowId", "stepName", "eventName", "associations");
    }

    private static void registerSnapshotStore(ComponentRegistry componentRegistry, Converter converter) {
        componentRegistry.registerComponent(SnapshotStore.class, c -> new InMemorySnapshotStore(converter));
    }

    /**
     * Publishes two started workflows that each wait: one for a named event with an association, and one whose wait
     * names no event, so that its row matches every routed event.
     */
    private void publishTwoWaitingWorkflows() {
        publish(startedWorkflow("wf-1"));
        publish(startedWait("wf-1", "awaitPayment", "PaymentReceived", Set.of("payload:orderId=wf-1")));
        publish(startedWorkflow("wf-2"));
        publish(startedWait("wf-2", "awaitUnreadable", "", Set.of()));
    }

    private EventMessage startedWorkflow(String workflowId) {
        return EventMessageUtils.startedWorkflow(workflowExecutionOperations(workflowId),
                                                 "OrderWorkflow",
                                                 DEFINITION_ID,
                                                 DefaultEventNameCustomizer.Builder.defaults());
    }

    private EventMessage startedWait(String workflowId, String stepName, String eventName, Set<String> associations) {
        var event = (GenericEventMessage) EventMessageUtils.startedWaitForEventStep(
                workflowExecutionOperations(workflowId),
                stepName,
                new StepWaitInfo(STARTED_AT, eventName, associations, STARTED_AT.plusSeconds(60)),
                DefaultEventNameCustomizer.Builder.defaults()
        );
        // The engine attaches the event converter of its processing context, which the base class mocks.
        return event.withConverter(configuration.getComponent(EventConverter.class));
    }

    private EventSourcedWaitAssociations load() {
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("load-wait-associations-test")
                            .executeWithResult(context -> repository(EventSourcedWaitAssociations.class)
                                    .loadOrCreate(EventSourcedWaitAssociations.ENTITY_ID, context)
                                    .thenApply(ManagedEntity::entity))
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }

    @Nullable
    private Snapshot storedSnapshot() {
        var snapshotName = configuration.getComponent(MessageTypeResolver.class)
                                        .resolveOrThrow(EventSourcedWaitAssociations.class)
                                        .qualifiedName();
        return FutureUtils.joinAndUnwrap(configuration.getComponent(SnapshotStore.class).load(
                snapshotName, EventSourcedWaitAssociations.ENTITY_ID, null
        ));
    }
}
