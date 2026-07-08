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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinitionId;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext;
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.modelling.repository.Repository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkflowStateRepositoryIntegrationTest {

    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void repositoryRebuildsWorkflowStateFromWorkflowIdTaggedEvents() {
        configuration = configuration();
        configuration.start();

        var definitionId = new WorkflowDefinitionId(new QualifiedName("OrderWorkflow"), "1.0.0");
        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var context = workflowContext("wf-1", "1.0.0");

        publish(EventMessageUtils.startedWorkflow(context, "OrderWorkflow", definitionId, customizer));
        publish(EventMessageUtils.completedStep(
                context,
                "approveOrder",
                Map.of("approved", true),
                io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME,
                customizer
        ));

        var state = load("wf-1");
        assertThat(state.workflowId()).isEqualTo("wf-1");
        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
        assertThat(state.payload()).containsEntry("orderId", "wf-1")
                                   .containsEntry("approved", true);
        assertThat(state.workflowDefinitionId()).isEqualTo(definitionId);
        assertThat(state.workflowDefinitionVersion()).isEqualTo("1.0.0");
        assertThat(state.getStep("approveOrder").status())
                .isEqualTo(io.axoniq.workflow.runtime.api.execution.status.StepStatus.COMPLETED);
    }

    private AxonConfiguration configuration() {
        var module = WorkflowModule.defaults("workflow-state-module", TestContext.class)
                                   .workflowContextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName("workflow-state")
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   );

        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerModule(module));
        return configurer.build();
    }

    private void publish(EventMessage eventMessage) {
        var eventStore = configuration.getComponent(org.axonframework.eventsourcing.eventstore.EventStore.class);
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        unitOfWorkFactory.create("publish-workflow-state-test")
                         .executeWithResult(context -> eventStore.publish(context, eventMessage)
                                                                 .thenApply(ignored -> eventMessage))
                         .join();
    }

    private EventSourcedWorkflowState load(String workflowId) {
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create("load-workflow-state-test")
                                .executeWithResult(context -> repository().loadOrCreate(workflowId, context)
                                                                          .thenApply(managedEntity -> managedEntity.entity()))
                                .join();
    }

    @SuppressWarnings("unchecked")
    private Repository<String, EventSourcedWorkflowState> repository() {
        return configuration.getComponents(Repository.class)
                            .values()
                            .stream()
                            .filter(repository -> repository.entityType().equals(EventSourcedWorkflowState.class))
                            .map(repository -> (Repository<String, EventSourcedWorkflowState>) repository)
                            .findFirst()
                            .orElseThrow();
    }

    private static WorkflowContext workflowContext(String workflowId, String version) {
        var context = mock(WorkflowContext.class);
        when(context.workflowId()).thenReturn(workflowId);
        when(context.workflowPayload()).thenReturn(Map.of("orderId", workflowId));
        when(context.workflowVersion()).thenReturn(version);
        return context;
    }

    static class TestContext extends AbstractDSLWorkflowContext {

        public TestContext(@Nonnull Map<String, Object> payload,
                           @Nonnull String workflowId,
                           @Nonnull ProcessingContext processingContext,
                           @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
