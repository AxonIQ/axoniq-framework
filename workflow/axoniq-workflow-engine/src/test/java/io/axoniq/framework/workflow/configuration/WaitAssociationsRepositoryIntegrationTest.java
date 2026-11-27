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
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
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

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    @Test
    void repositoryStoresNoSnapshotEvenWhenSnapshotsAreEnabled() {
        // given
        configuration = configuration(MODULE, configurer -> configurer
                .componentRegistry(cr -> registerSnapshotStore(cr, new JacksonConverter()))
                .snapshots(snapshots -> snapshots.enabled(true)));
        configuration.start();
        publishTwoWaitingWorkflows();

        // when
        var openWaits = load();

        // then
        assertThat(openWaits.waitAssociations()).hasSize(2);
        assertThat(storedSnapshot()).isNull();
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
