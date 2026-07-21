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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.management.WorkflowManager.CancellationReason;
import io.axoniq.workflow.runtime.api.management.WorkflowManager.WorkflowQuery;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultWorkflowManager#cancel(WorkflowQuery, CancellationReason)} covering the
 * matched-but-not-cancelled and no-match cases that the whole-workflow (as opposed to per-step) API exposes.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class DefaultWorkflowManagerTest {

    @Test
    void cancel_onAlreadyTerminalWorkflow_matchesButDoesNotCancel() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        when(execution.state()).thenReturn(state);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.CANCELLED); // terminal
        when(repository.findById("wf-terminal")).thenReturn(Optional.of(execution));

        var manager = new DefaultWorkflowManager(repository);
        var result = manager.cancel(new WorkflowQuery.ById("wf-terminal"), CancellationReason.of("too late"));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.cancelled()).isEqualTo(0);
        assertThat(result.workflowIds()).isEmpty();
        verify(execution, never()).requestWorkflowCancellation(any());
    }

    @Test
    void cancel_onUnknownId_matchesNothing() {
        var repository = mock(WorkflowExecutionRepository.class);
        when(repository.findById("no-such")).thenReturn(Optional.empty());

        var manager = new DefaultWorkflowManager(repository);
        var result = manager.cancel(new WorkflowQuery.ById("no-such"), CancellationReason.none());

        assertThat(result.matched()).isEqualTo(0);
        assertThat(result.cancelled()).isEqualTo(0);
        assertThat(result.workflowIds()).isEmpty();
    }
}
