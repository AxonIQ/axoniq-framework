/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RunningWorkflows integration test.
 *
 * @author Simon Zambrovski
 */
class RunningWorkflowsRepositoryIntegrationTest extends AbstractEventSourcedEntityRepositoryTestBase {

    @Test
    void repositoryRebuildsRunningWorkflowIdsFromLifecycleTags() {
        configuration = configuration("running-workflows-module");
        configuration.start();

        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var definitionId = VersionedType.of(new QualifiedName("OrderWorkflow"), "0.0.1");
        var first = workflowExecutionOperations("wf-1");
        var second = workflowExecutionOperations("wf-2");

        publish(EventMessageUtils.startedWorkflow(first, "OrderWorkflow", definitionId, customizer));
        publish(EventMessageUtils.startedWorkflow(second, "OrderWorkflow", definitionId, customizer));
        publish(EventMessageUtils.completedWorkflow(first, "OrderWorkflow", definitionId, customizer));

        assertThat(lifecycleEvents()).hasSize(3);
        assertThat(load().workflowIds()).isEqualTo(Set.of("wf-2"));

        publish(EventMessageUtils.timeoutWorkflow(
                second,
                "OrderWorkflow",
                Instant.parse("2026-07-08T12:00:00Z"),
                definitionId,
                customizer
        ));

        assertThat(load().workflowIds()).isEmpty();
    }

    @Test
    void repositoryCreatesEmptyStateWhenNoLifecycleEventsExist() {
        configuration = configuration("running-workflows-module");
        configuration.start();

        assertThat(load().workflowIds()).isEmpty();
    }

    @Nested
    class Snapshots {

        private static final VersionedType DEFINITION_ID =
                VersionedType.of(new QualifiedName("OrderWorkflow"), "0.0.1");
        private static final TypeReference<Map<String, Object>> FIELDS = new TypeReference<>() {
        };

        static Stream<Arguments> converters() {
            return Stream.of(Arguments.of(Named.of("JacksonConverter", new JacksonConverter())),
                             Arguments.of(Named.of("Jackson2Converter", new Jackson2Converter())),
                             Arguments.of(Named.of("JacksonConverter reading fields", fieldReadingConverter())));
        }

        @ParameterizedTest
        @MethodSource("converters")
        void repositoryRestoresRunningWorkflowIdsFromSnapshot(Converter converter) {
            // given
            configuration = configuration("running-workflows-module", configurer -> configurer
                    .componentRegistry(cr -> registerSnapshotStore(cr, converter))
                    .snapshots(snapshots -> snapshots.enabled(true).runningWorkflows(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishTwoStartedAndOneCompletedWorkflow();

            // when - sourcing more events than the policy allows stores a snapshot
            load();

            // then - the snapshot went through the converter and still holds the running workflow
            var snapshot = storedSnapshot();
            assertThat(snapshot).isNotNull();
            assertThat(((EventSourcedRunningWorkflows) snapshot.payload()).workflowIds()).contains("wf-2");

            // and - a later load continues from the snapshot
            publish(startedWorkflow("wf-3"));
            assertThat(load().workflowIds()).isEqualTo(Set.of("wf-2", "wf-3"));
        }

        @Test
        void repositoryStoresNoSnapshotWhenSnapshotsAreNotEnabled() {
            // given
            configuration = configuration(
                    "running-workflows-module",
                    configurer -> configurer.componentRegistry(cr -> registerSnapshotStore(cr, new JacksonConverter()))
            );
            configuration.start();
            publishTwoStartedAndOneCompletedWorkflow();

            // when
            var runningWorkflows = load();

            // then
            assertThat(runningWorkflows.workflowIds()).isEqualTo(Set.of("wf-2"));
            assertThat(storedSnapshot()).isNull();
        }

        @Test
        void startFailsWhenSnapshotsAreEnabledWithoutSnapshotStore() {
            // given
            configuration = configuration(
                    "running-workflows-module", configurer -> configurer.snapshots(snapshots -> snapshots.enabled(true))
            );

            // when / then
            assertThatThrownBy(() -> configuration.start())
                    .hasStackTraceContaining("A SnapshotStore must be configured to use snapshotting.");
        }

        @ParameterizedTest
        @MethodSource("converters")
        void convertedSnapshotHoldsOnlyTheWorkflowIds(Converter converter) {
            // given
            configuration = configuration("running-workflows-module");
            configuration.start();
            publishTwoStartedAndOneCompletedWorkflow();
            var runningWorkflows = load();

            // when
            byte[] stored = converter.convert(runningWorkflows, byte[].class);

            // then - no other getter of the entity ends up in the snapshot
            Map<String, Object> fields = converter.convert(stored, FIELDS.getType());
            assertThat(fields).containsOnlyKeys("workflowIds");
        }

        private static void registerSnapshotStore(ComponentRegistry componentRegistry, Converter converter) {
            componentRegistry.registerComponent(SnapshotStore.class, c -> new InMemorySnapshotStore(converter));
        }

        private void publishTwoStartedAndOneCompletedWorkflow() {
            publish(startedWorkflow("wf-1"));
            publish(startedWorkflow("wf-2"));
            publish(EventMessageUtils.completedWorkflow(
                    workflowExecutionOperations("wf-1"),
                    "OrderWorkflow",
                    DEFINITION_ID,
                    DefaultEventNameCustomizer.Builder.defaults()
            ));
        }

        private EventMessage startedWorkflow(String workflowId) {
            return EventMessageUtils.startedWorkflow(
                    workflowExecutionOperations(workflowId),
                    "OrderWorkflow",
                    DEFINITION_ID,
                    DefaultEventNameCustomizer.Builder.defaults()
            );
        }

        @Nullable
        private Snapshot storedSnapshot() {
            var snapshotName = configuration.getComponent(MessageTypeResolver.class)
                                            .resolveOrThrow(EventSourcedRunningWorkflows.class)
                                            .qualifiedName();
            return FutureUtils.joinAndUnwrap(configuration.getComponent(SnapshotStore.class).load(
                    snapshotName, EventSourcedRunningWorkflows.ENTITY_ID, null
            ));
        }
    }

    private EventSourcedRunningWorkflows load() {
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create("load-running-workflows-test")
                                .executeWithResult(context -> repository(EventSourcedRunningWorkflows.class)
                                        .loadOrCreate(
                                                EventSourcedRunningWorkflows.ENTITY_ID,
                                                context
                                        )
                                        .thenApply(managedEntity -> managedEntity.entity()))
                                .join();
    }

    private List<EventMessage> lifecycleEvents() {
        var eventStorageEngine = configuration.getComponent(EventStorageEngine.class);
        MessageStream<EventMessage> stream = eventStorageEngine.source(
                SourcingCondition.conditionFor(
                        EventCriteria.havingTags(Tag.of(
                                TAG_WORKFLOW_EVENT_TYPE,
                                TAG_VALUE_EVENT_TYPE_LIFECYCLE
                        ))
                )
        );
        try {
            return stream.reduce(new ArrayList<EventMessage>(), (events, entry) -> {
                EventMessage event = entry.message();
                if (!(event instanceof TerminalEventMessage)) {
                    events.add(event);
                }
                return events;
            }).join();
        } finally {
            stream.close();
        }
    }
}
