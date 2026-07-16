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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests interrupt handling while delegates wait for a just-appended STARTED event.
 *
 * @author Stefan Dragisic
 */
class DelegateInterruptedAwaitTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private EventNameCustomizer parentCustomizer;
    private WaitForDelegate waitForDelegate;
    private ExecuteDelegate executeDelegate;

    @BeforeEach
    void setUp() throws InterruptedException {
        Thread.interrupted();

        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        EventSink eventSink = mock(EventSink.class);
        UnitOfWorkFactory unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        Executor executor = Runnable::run;
        parentCustomizer = DefaultEventNameCustomizer.Builder.defaults();

        when(workflowExecution.state()).thenReturn(state);
        when(workflowExecution.isExecutable()).thenReturn(true);
        when(workflowExecution.hasTasks()).thenReturn(true);
        when(state.workflowStepNames()).thenReturn(List.of());
        doThrow(new InterruptedException("workflow interrupted"))
                .when(workflowExecution)
                .awaitStateChange(any());

        waitForDelegate = new WaitForDelegate(
                workflowContext,
                workflowExecution,
                parentCustomizer,
                Clock.systemUTC(),
                unitOfWorkFactory,
                eventSink,
                executor,
                new DefaultWorkflowScheduler(Clock.systemUTC())
        );
        executeDelegate = new ExecuteDelegate(
                workflowContext,
                workflowExecution,
                parentCustomizer,
                Clock.systemUTC(),
                unitOfWorkFactory,
                eventSink,
                executor,
                new DefaultWorkflowScheduler(Clock.systemUTC()),
                new DefaultExecuteStepActionResolver()
        );
    }

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void waitForEventReturnsCanceledResultAndRestoresInterruptWhenStartedAwaitIsInterrupted() {
        String stepName = "waitForApproval";
        when(state.containsStep(stepName)).thenReturn(false);

        WorkflowStepResult result = waitForDelegate.waitForEvent(waitForCommand(stepName));

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(result.getStepName()).isEqualTo(stepName);
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.canceled()).isTrue();
        assertThat(result.failure()).isFalse();
        assertThat(result.error()).isEmpty();
    }

    @Test
    void executeReturnsCanceledResultAndRestoresInterruptWhenStartedAwaitIsInterrupted() {
        String stepName = "chargePayment";
        when(state.containsStep(stepName)).thenReturn(false);

        WorkflowStepResult result = executeDelegate.execute(executeCommand(stepName));

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(result.getStepName()).isEqualTo(stepName);
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.canceled()).isTrue();
        assertThat(result.failure()).isFalse();
        assertThat(result.error()).isEmpty();
    }

    private PrimitiveCommands.WorkflowStepResultWaitForCommand waitForCommand(String stepName) {
        EventCondition eventCondition = mock(EventCondition.class);
        return new PrimitiveCommands.WorkflowStepResultWaitForCommand(
                stepName,
                eventCondition,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofSeconds(5),
                DefaultEventNameCustomizer.Builder.defaults()
        );
    }

    private PrimitiveCommands.WorkflowStepResultExecuteCommand executeCommand(String stepName) {
        PayloadProcessor action = (ctx, payload) -> Map.of("result", "done");
        return new PrimitiveCommands.WorkflowStepResultExecuteCommand(
                stepName,
                Map.of(),
                action,
                LocalOnlyPayloadReducer.INSTANCE,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofSeconds(5),
                DefaultEventNameCustomizer.Builder.defaults(),
                RetryPolicy.NONE
        );
    }
}
