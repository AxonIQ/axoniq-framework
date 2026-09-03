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
package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkflowStateUtilsTest {

    @Test
    void constructorIsPrivate() throws NoSuchMethodException {
        Constructor<WorkflowStateUtils> constructor = WorkflowStateUtils.class.getDeclaredConstructor();
        assertThat(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())).isTrue();
        constructor.setAccessible(true);
        assertThatCode(constructor::newInstance).doesNotThrowAnyException();
    }

    @Test
    void matchesStepReturnsFalseWhenStepNotContained() {
        WorkflowState state = mock(WorkflowState.class);
        when(state.getStep("step1")).thenReturn(null);

        assertThat(WorkflowStateUtils.matchesStep(state, "step1", step -> true)).isFalse();
    }

    @Test
    void matchesStepEvaluatesPredicateWhenStepExists() {
        WorkflowState state = mock(WorkflowState.class);
        WorkflowStep step = mock(WorkflowStep.class);
        when(state.getStep("step1")).thenReturn(step);
        when(step.status()).thenReturn(StepStatus.STARTED);

        assertThat(WorkflowStateUtils.matchesStep(state, "step1", s -> s.status() == StepStatus.STARTED)).isTrue();
        assertThat(WorkflowStateUtils.matchesStep(state, "step1", s -> s.status() == StepStatus.COMPLETED)).isFalse();
    }

    @Test
    void isStepTerminalReturnsFalseWhenStepNotContained() {
        WorkflowState state = mock(WorkflowState.class);
        when(state.getStep("step1")).thenReturn(null);

        assertThat(WorkflowStateUtils.isStepTerminal(state, "step1")).isFalse();
        assertThat(WorkflowStateUtils.stepTerminal("step1").test(state)).isFalse();
    }

    @Test
    void isStepTerminalReturnsFalseWhenStepNotTerminal() {
        WorkflowState state = mock(WorkflowState.class);
        WorkflowStep step = mock(WorkflowStep.class);

        when(state.getStep("step1")).thenReturn(step);
        when(step.status()).thenReturn(StepStatus.STARTED);

        assertThat(WorkflowStateUtils.isStepTerminal(state, "step1")).isFalse();
        assertThat(WorkflowStateUtils.stepTerminal("step1").test(state)).isFalse();
    }

    @Test
    void isStepTerminalReturnsTrueWhenStepIsTerminal() {
        for (StepStatus terminalStatus : List.of(StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.TIMED_OUT, StepStatus.CANCELLED)) {
            WorkflowState state = mock(WorkflowState.class);
            WorkflowStep step = mock(WorkflowStep.class);

            when(state.getStep("step1")).thenReturn(step);
            when(step.status()).thenReturn(terminalStatus);

            assertThat(WorkflowStateUtils.isStepTerminal(state, "step1")).isTrue();
            assertThat(WorkflowStateUtils.stepTerminal("step1").test(state)).isTrue();
        }
    }

    @Test
    void isStepActiveReturnsFalseWhenStepNotContained() {
        WorkflowState state = mock(WorkflowState.class);
        when(state.getStep("step1")).thenReturn(null);

        assertThat(WorkflowStateUtils.isStepActive(state, "step1")).isFalse();
        assertThat(WorkflowStateUtils.stepActive("step1").test(state)).isFalse();
    }

    @Test
    void isStepActiveReturnsTrueWhenStepStartedOrRetrying() {
        for (StepStatus activeStatus : List.of(StepStatus.STARTED, StepStatus.RETRYING)) {
            WorkflowState state = mock(WorkflowState.class);
            WorkflowStep step = mock(WorkflowStep.class);

            when(state.getStep("step1")).thenReturn(step);
            when(step.status()).thenReturn(activeStatus);

            assertThat(WorkflowStateUtils.isStepActive(state, "step1")).isTrue();
            assertThat(WorkflowStateUtils.stepActive("step1").test(state)).isTrue();
        }
    }

    @Test
    void isStepActiveReturnsFalseWhenStepIsTerminal() {
        for (StepStatus terminalStatus : List.of(StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.TIMED_OUT, StepStatus.CANCELLED)) {
            WorkflowState state = mock(WorkflowState.class);
            WorkflowStep step = mock(WorkflowStep.class);

            when(state.getStep("step1")).thenReturn(step);
            when(step.status()).thenReturn(terminalStatus);

            assertThat(WorkflowStateUtils.isStepActive(state, "step1")).isFalse();
            assertThat(WorkflowStateUtils.stepActive("step1").test(state)).isFalse();
        }
    }

    @Test
    void isStepStatusMatchesCorrectStatus() {
        WorkflowState state = mock(WorkflowState.class);
        WorkflowStep step = mock(WorkflowStep.class);

        when(state.getStep("step1")).thenReturn(step);
        when(step.status()).thenReturn(StepStatus.STARTED);

        assertThat(WorkflowStateUtils.isStepStatus(state, "step1", StepStatus.STARTED)).isTrue();
        assertThat(WorkflowStateUtils.isStepStatus(state, "step1", StepStatus.COMPLETED)).isFalse();
        assertThat(WorkflowStateUtils.stepStatus("step1", StepStatus.STARTED).test(state)).isTrue();
        assertThat(WorkflowStateUtils.stepStatus("step1", StepStatus.COMPLETED).test(state)).isFalse();
    }
}
