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
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.*;

import java.util.Map;

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

        @Test
        void repositoryRestoresWorkflowStateFromSnapshot() {
            // given
            configuration = configuration("workflow-state-module", configurer -> configurer
                    .componentRegistry(cr -> cr.registerComponent(
                            SnapshotStore.class, c -> new InMemorySnapshotStore(new JacksonConverter())
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
