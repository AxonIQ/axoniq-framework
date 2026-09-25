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
 * https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 * https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class WorkflowExecutionOperationsDelegationTest {

    private static final Map<String, Object> PAYLOAD = Map.of("orderId", "order-1");

    private WorkflowExecutionOperationsDelegation delegation;
    private WorkflowExecution workflowExecution;
    private WorkflowState workflowState;
    private WorkflowContext workflowContext;
    private ProcessingContext processingContext;
    private EventStore eventStore;
    private Clock clock;
    private UnitOfWorkFactory unitOfWorkFactory;
    private ExecutorService executorService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        var workflowConfiguration = mock(WorkflowConfiguration.class);
        workflowExecution = mock(WorkflowExecution.class);
        workflowState = mock(WorkflowState.class);
        workflowContext = mock(WorkflowContext.class);
        processingContext = mock(ProcessingContext.class);
        eventStore = mock(EventStore.class);
        clock = Clock.systemUTC();
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        executorService = mock(ExecutorService.class);

        when(workflowConfiguration.eventNameCustomizer()).thenReturn(defaults());
        when(workflowExecution.state()).thenReturn(workflowState);
        when(workflowExecution.workflowId()).thenReturn("workflow-id");
        when(workflowExecution.workflowName()).thenReturn("Order workflow");
        when(workflowState.workflowDefinitionId()).thenReturn(
                VersionedType.of(new QualifiedName("OrderWorkflow"), "1.2.3")
        );
        when(workflowState.payload()).thenReturn(PAYLOAD);
        when(workflowState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(workflowState.workflowStepNames()).thenReturn(List.of("reserve-stock", "ship-order"));
        when(processingContext.component(UnitOfWorkFactory.class)).thenReturn(unitOfWorkFactory);
        when(processingContext.component(Clock.class)).thenReturn(clock);
        when(processingContext.component(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR)).thenReturn(executorService);
        when(processingContext.component(EventStore.class)).thenReturn(eventStore);
        when(processingContext.component(WorkflowScheduler.class)).thenReturn(new ControllableWorkflowScheduler());
        when(processingContext.component(ExecuteStepActionResolver.class)).thenReturn(
                new DefaultExecuteStepActionResolver()
        );

        delegation = new WorkflowExecutionOperationsDelegation(
                workflowConfiguration,
                workflowContext,
                workflowExecution,
                new RunningSteps(),
                new EventWaitConditions(),
                new ReachedSteps(),
                mock(WorkflowTerminalTransition.class),
                processingContext
        );
    }

    @Nested
    class ExecutionData {

        @Test
        void exposesTheDataOfTheUnderlyingExecution() {
            // when
            var workflowId = delegation.workflowId();
            var workflowVersion = delegation.workflowVersion();
            var workflowPayload = delegation.workflowPayload();
            var workflowStatus = delegation.workflowStatus();
            var workflowStepNames = delegation.workflowStepNames();

            // then
            assertThat(workflowId).isEqualTo("workflow-id");
            assertThat(workflowVersion).isEqualTo("1.2.3");
            assertThat(workflowPayload).isSameAs(PAYLOAD);
            assertThat(workflowStatus).isEqualTo(WorkflowStatus.STARTED);
            assertThat(workflowStepNames).containsExactly("reserve-stock", "ship-order");
        }

        @Test
        void exposesTheConfiguredRuntimeServices() {
            assertThat(delegation.processingContext()).isSameAs(processingContext);
            assertThat(delegation.eventStore()).isSameAs(eventStore);
            assertThat(delegation.clock()).isSameAs(clock);
            assertThat(delegation.unitOfWorkFactory()).isSameAs(unitOfWorkFactory);
            assertThat(delegation.workflowBodyUnitOfWorkFactory()).isNotSameAs(unitOfWorkFactory);
            assertThat(delegation.executorService()).isSameAs(executorService);
        }
    }

    @Nested
    class WorkflowContextAccess {

        @Test
        void returnsTheAuthorFacingContext() {
            WorkflowContext typedWorkflowContext = delegation.typedWorkflowContext();

            assertThat(typedWorkflowContext).isSameAs(workflowContext);
        }
    }

    @Nested
    class Description {

        @Test
        void describesTheUnderlyingExecution() {
            var descriptor = mock(ComponentDescriptor.class);

            delegation.describeTo(descriptor);

            verify(descriptor).describeProperty("workflowId", "workflow-id");
            verify(descriptor).describeProperty("workflowName", "Order workflow");
        }
    }

    @Nested
    class TerminalState {

        @Test
        void preventsPrimitivesFromRunningAfterTermination() {
            var terminalCause = new IllegalStateException("workflow has terminated");
            doThrow(terminalCause).when(workflowState).throwTerminalCause();

            assertThatThrownBy(() -> delegation.modifyPayload(null)).isSameAs(terminalCause);
            assertThatThrownBy(() -> delegation.execute(null)).isSameAs(terminalCause);
            assertThatThrownBy(() -> delegation.waitForEvent(null)).isSameAs(terminalCause);
            assertThatThrownBy(() -> delegation.version(null)).isSameAs(terminalCause);
            assertThatThrownBy(() -> delegation.publish(null)).isSameAs(terminalCause);
            assertThatThrownBy(() -> delegation.cancelWorkflow(null)).isSameAs(terminalCause);
            assertThatThrownBy(() -> delegation.failWorkflow(null)).isSameAs(terminalCause);

            verify(workflowState, times(7)).throwTerminalCause();
        }
    }
}
