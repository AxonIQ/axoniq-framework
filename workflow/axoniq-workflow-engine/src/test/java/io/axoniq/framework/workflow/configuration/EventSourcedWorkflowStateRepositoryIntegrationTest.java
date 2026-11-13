/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static io.axoniq.framework.workflow.dsl.api.StepStatus.COMPLETED;
import static io.axoniq.framework.workflow.dsl.api.StepStatus.FAILED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * EventSourcedWorkflowState integration test.
 *
 * @author Simon Zambrovski
 */
class EventSourcedWorkflowStateRepositoryIntegrationTest extends AbstractEventSourcedEntityRepositoryTestBase {

    @Test
    void repositoryRebuildsWorkflowStateFromWorkflowIdTaggedEvents() {
        configuration = configuration("workflow-state-module");
        configuration.start();

        var definitionId = VersionedType.of(new QualifiedName("OrderWorkflow"), "1.0.0");
        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var context = workflowExecutionOperations("wf-1", "1.0.0");

        publish(EventMessageUtils.startedWorkflow(context, "OrderWorkflow", definitionId, customizer));
        publish(EventMessageUtils.completedStep(
                context,
                "approveOrder",
                Map.of("approved", true),
                io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME,
                customizer
        ));

        var state = load("wf-1");
        assertThat(state.workflowId()).isEqualTo("wf-1");
        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
        assertThat(state.payload()).containsEntry("orderId", "wf-1")
                                   .containsEntry("approved", true);
        assertThat(state.workflowDefinitionId()).isEqualTo(definitionId);
        assertThat(state.workflowDefinitionId().version()).isEqualTo("1.0.0");
        assertThat(state.getStep("approveOrder").status())
                .isEqualTo(COMPLETED);
    }

    @Nested
    class Snapshots {

        private static final TypeReference<Map<String, Object>> FIELDS = new TypeReference<>() {
        };

        static Stream<Arguments> converters() {
            return Stream.of(Arguments.of(Named.of("JacksonConverter", new JacksonConverter())),
                             Arguments.of(Named.of("Jackson2Converter", new Jackson2Converter())));
        }

        @ParameterizedTest
        @MethodSource("converters")
        void repositoryRestoresWorkflowStateFromSnapshot(Converter converter) {
            // given
            configuration = configuration("workflow-state-module", configurer -> configurer
                    .componentRegistry(cr -> cr.registerComponent(
                            SnapshotStore.class, c -> new InMemorySnapshotStore(converter)
                    ))
                    .snapshots(snapshots -> snapshots.enabled(true).workflowState(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            var definitionId = VersionedType.of(new QualifiedName("OrderWorkflow"), "1.0.0");
            var customizer = DefaultEventNameCustomizer.Builder.defaults();
            var context = workflowExecutionOperations("wf-1", "1.0.0");
            publish(EventMessageUtils.startedWorkflow(context, "OrderWorkflow", definitionId, customizer));
            publish(EventMessageUtils.completedStep(
                    context,
                    "approveOrder",
                    Map.of("approved", true),
                    CombineGlobalAndLocalPayloadReducer.NAME,
                    customizer
            ));
            publish(EventMessageUtils.failStep(
                    context, "chargeCard", new IllegalStateException("card declined"), customizer
            ));

            // when - the first load sources more events than the policy allows, so it stores a snapshot
            load("wf-1");
            var restored = load("wf-1");

            // then
            var snapshotName = configuration.getComponent(MessageTypeResolver.class)
                                            .resolveOrThrow(EventSourcedWorkflowState.class)
                                            .qualifiedName();
            assertThat(FutureUtils.joinAndUnwrap(
                    configuration.getComponent(SnapshotStore.class).load(snapshotName, "wf-1", null)
            )).isNotNull();
            assertThat(restored.workflowId()).isEqualTo("wf-1");
            assertThat(restored.workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
            assertThat(restored.workflowDefinitionId()).isEqualTo(definitionId);
            assertThat(restored.payload()).containsEntry("orderId", "wf-1")
                                          .containsEntry("approved", true);
            assertThat(restored.getStep("approveOrder").status()).isEqualTo(COMPLETED);
            assertThat(restored.getStep("chargeCard").status()).isEqualTo(FAILED);
            assertThat(restored.getStep("chargeCard").error()).hasMessage("card declined");
        }

        @ParameterizedTest
        @MethodSource("converters")
        void convertedSnapshotHoldsOnlyTheStateSnapshot(Converter converter) {
            // given
            configuration = configuration("workflow-state-module");
            configuration.start();
            var definitionId = VersionedType.of(new QualifiedName("OrderWorkflow"), "1.0.0");
            var customizer = DefaultEventNameCustomizer.Builder.defaults();
            var context = workflowExecutionOperations("wf-1", "1.0.0");
            publish(EventMessageUtils.startedWorkflow(context, "OrderWorkflow", definitionId, customizer));
            publish(EventMessageUtils.failStep(
                    context, "chargeCard", new IllegalStateException("card declined"), customizer
            ));
            var state = load("wf-1");

            // when
            byte[] stored = converter.convert(state, byte[].class);

            // then - no other getter of the entity ends up in the snapshot
            Map<String, Object> fields = converter.convert(stored, FIELDS.getType());
            assertThat(fields).containsOnlyKeys("stateSnapshot");
        }
    }

    private EventSourcedWorkflowState load(String workflowId) {
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create("load-workflow-state-test")
                                .executeWithResult(context -> repository(EventSourcedWorkflowState.class)
                                        .loadOrCreate(workflowId, context)
                                        .thenApply(managedEntity -> managedEntity.entity()))
                                .join();
    }
}
