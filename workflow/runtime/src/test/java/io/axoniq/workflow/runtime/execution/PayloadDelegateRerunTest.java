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

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

/**
 * Regression test for the replay-skip gate in {@link PayloadDelegate#modifyPayload}.
 * <p>
 * {@code modifyPayload} publishes a COMPLETED terminal step event directly. On a post-crash live re-run
 * (replay) the step is already present in the event-sourced {@link WorkflowState}, so re-publishing it would
 * produce a duplicate terminal step record. The fix gates the {@code appendTask}/publish on
 * {@code !state().containsStep(stepName)} — the same replay-skip gate the other primitives have — so the
 * terminal step is published exactly once across a re-run.
 *
 * @author Stefan Dragisic
 */
class PayloadDelegateRerunTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private PayloadDelegate delegate;
    private ReachedSteps reachedSteps;

    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        reachedSteps = new ReachedSteps();
        ProcessingContext processingContext = mock(ProcessingContext.class);
        EventSink eventSink = mock(EventSink.class);
        UnitOfWorkFactory unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        Executor executor = Runnable::run;
        EventNameCustomizer parent = DefaultEventNameCustomizer.Builder.defaults();

        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.state()).thenReturn(state);
        when(workflowExecution.processingContext()).thenReturn(processingContext);
        when(state.payload()).thenReturn(Map.of());

        delegate = new PayloadDelegate(
                workflowContext, workflowExecution, new RunningSteps(), reachedSteps, parent,
                Clock.systemUTC(), unitOfWorkFactory, eventSink, executor, new ControllableWorkflowScheduler()
        );
    }

    /**
     * Post-crash live re-run: the step already exists in the event-sourced state as a COMPLETED terminal
     * step. {@code modifyPayload} must NOT append a publishing task again, otherwise a duplicate terminal
     * step record would be produced. With the gate, exactly zero new tasks are appended on the re-run.
     */
    @Test
    void modifyPayloadDoesNotRepublishWhenStepAlreadyInStateOnRerun() {
        String stepName = "payloadStep";
        // Replay/re-run: the COMPLETED terminal step is already projected into the event-sourced state.
        when(state.containsStep(stepName)).thenReturn(true);
        when(state.getStep(stepName)).thenReturn(completedStep(stepName));

        PayloadModification modification = p -> p;
        EventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

        delegate.modifyPayload(PrimitiveCommands.modifyPayload(stepName, modification, customizer));

        // No re-publish: the gate skips appendTask when the step is already present, so no duplicate
        // terminal step record is produced on the re-run. The drift guard is likewise skipped.
        verify(workflowExecution, never()).appendTask(any());
    }

    /**
     * First live run: the step is not yet in state, so {@code modifyPayload} appends exactly one publishing
     * task (after passing the drift guard). This is the sole publish that, on a subsequent re-run, must not
     * be repeated.
     */
    @Test
    void modifyPayloadPublishesOnceOnFirstLiveRun() {
        String stepName = "payloadStep";
        when(state.containsStep(stepName)).thenReturn(false);

        PayloadModification modification = p -> p;
        EventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

        delegate.modifyPayload(PrimitiveCommands.modifyPayload(stepName, modification, customizer));

        verify(workflowExecution, times(1)).appendTask(any());
    }

    private WorkflowStep completedStep(String name) {
        return new WorkflowStep(name, StepStatus.COMPLETED, null, null, Instant.now(), null);
    }
}
