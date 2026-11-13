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
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.*;

import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.StepStatus.COMPLETED;
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

    private EventSourcedWorkflowState load(String workflowId) {
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create("load-workflow-state-test")
                                .executeWithResult(context -> repository(EventSourcedWorkflowState.class)
                                        .loadOrCreate(workflowId, context)
                                        .thenApply(managedEntity -> managedEntity.entity()))
                                .join();
    }
}
