/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.TerminatePrimitive.TerminateCommand;
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import org.junit.jupiter.api.*;
import org.mockito.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowExecution#cancel()} default methods.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class WorkflowExecutionCancelTest {

    private WorkflowExecution execution;
    private WorkflowContext workflowContext;

    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        execution = mock(WorkflowExecution.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
        when(execution.workflowContext()).thenReturn(workflowContext);
    }

    @Test
    void cancelWithReasonDelegatesToTerminate() {
        execution.cancel("Cancelled by admin");

        var captor = ArgumentCaptor.forClass(TerminateCommand.class);
        verify(workflowContext).terminate(captor.capture());

        var command = captor.getValue();
        assertThat(command.error()).isFalse();
        assertThat(command.cause()).isInstanceOf(WorkflowCancelledException.class);
        assertThat(command.cause().getMessage()).isEqualTo("Cancelled by admin");
        assertThat(command.stepName()).isNull();
    }

    @Test
    void cancelWithoutReasonDelegatesToTerminate() {
        execution.cancel();

        var captor = ArgumentCaptor.forClass(TerminateCommand.class);
        verify(workflowContext).terminate(captor.capture());

        var command = captor.getValue();
        assertThat(command.error()).isFalse();
        assertThat(command.cause()).isNull();
        assertThat(command.stepName()).isNull();
    }

    @Test
    void cancelWithExceptionDelegatesToTerminate() {
        var cause = new RuntimeException("Something went wrong");
        execution.cancel(cause);

        var captor = ArgumentCaptor.forClass(TerminateCommand.class);
        verify(workflowContext).terminate(captor.capture());

        var command = captor.getValue();
        assertThat(command.error()).isFalse();
        assertThat(command.cause()).isSameAs(cause);
        assertThat(command.stepName()).isNull();
    }
}
