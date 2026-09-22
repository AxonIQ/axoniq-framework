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
package io.axoniq.framework.workflow.runtime.test.fixture;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowEngineTestingState}.
 *
 * @author Simon Zambrovski
 */
class WorkflowEngineTestingStateTest {

    @BeforeEach
    void shortenAwaitilityTimeout() {
        Awaitility.setDefaultTimeout(Duration.ofMillis(50));
        Awaitility.setDefaultPollInterval(Duration.ofMillis(5));
        Awaitility.setDefaultPollDelay(Duration.ZERO);
    }

    @AfterEach
    void resetAwaitilityTimeout() {
        Awaitility.reset();
    }

    @Test
    void storesSelectedExecutionAndHistory() {
        MutableWorkflowEngineTestingState testingState = new MutableWorkflowEngineTestingState();
        WorkflowExecution execution = mock(WorkflowExecution.class);
        WorkflowState executionState = mock(WorkflowState.class);
        when(execution.state()).thenReturn(executionState);
        WorkflowState historyState = mock(WorkflowState.class);
        WorkflowHistory history = new WorkflowHistory("wf-1", historyState);

        testingState.setExecution(execution);
        testingState.setHistory(history);

        assertThat(testingState.execution()).isSameAs(execution);
        assertThat(testingState.history()).isSameAs(history);
        assertThat(testingState.state()).isSameAs(executionState);
        testingState.setExecution(null);
        assertThat(testingState.state()).isSameAs(historyState);

    }

    @Test
    void stateReportsWhenNothingIsSelected() {
        WorkflowEngineTestingState testingState = new MutableWorkflowEngineTestingState();

        assertThatThrownBy(testingState::state)
                .isInstanceOf(AssertionError.class)
                .hasMessage("No workflow execution or workflow history is currently selected.")
                .hasNoCause();
    }

    @Test
    void phaseNamedHelpersInspectWorkflowState() {

        var failureHandler = AssertionFailureHandler.handler(false);

        WorkflowState state = mock(WorkflowState.class);
        MutableWorkflowEngineTestingState testingState = new MutableWorkflowEngineTestingState();
        testingState.setHistory(new WorkflowHistory("wf-1", state));
        when(state.workflowStepNames()).thenReturn(List.of("reserve", "charge"));
        when(state.containsStep("reserve")).thenReturn(true);
        when(state.containsStep("charge")).thenReturn(true);
        when(state.getStep("reserve")).thenReturn(WorkflowStep.started("reserve",
                                                                       Map.of(),
                                                                       Instant.EPOCH,
                                                                       null));
        when(state.getStep("charge")).thenReturn(WorkflowStep.completed("charge",
                                                                        Map.of("ok", true),
                                                                        Instant.EPOCH,
                                                                        null));
        when(state.payload()).thenReturn(Map.of("orderId", "order-1"));

        testingState.waitingIn(failureHandler, "reserve");
        testingState.stepsPassed(failureHandler, "charge");
        testingState.hasSteps(failureHandler, "reserve", "charge");
        testingState.hasStepsInAnyOrder(failureHandler, "charge", "reserve");
        testingState.noStep("refund");
        testingState.payloadMatches(failureHandler, payload -> payload.containsKey("orderId"));
        testingState.workflowStateMatches(failureHandler, workflowState -> workflowState.payload().containsKey("orderId"));
    }

    @Test
    void consumerHelpersExposeMatchingState() {
        var state = workflowState(List.of("createUser", "activateUser2"),
                                  Map.of("id", "2"),
                                  step("createUser", StepStatus.COMPLETED),
                                  step("activateUser2", StepStatus.STARTED));
        AtomicReference<WorkflowStep> capturedStep = new AtomicReference<>();
        AtomicReference<Map<String, @Nullable Object>> capturedPayload = new AtomicReference<>();
        AtomicReference<WorkflowState> capturedState = new AtomicReference<>();

        state.stepSatisfies(step -> step.stepName().equals("activateUser2"), capturedStep::set);
        state.payloadSatisfies(capturedPayload::set);
        state.workflowStateSatisfies(capturedState::set);

        assertThat(capturedStep.get().stepName()).isEqualTo("activateUser2");
        assertThat(capturedPayload.get()).isEqualTo(Map.of("id", "2"));
        assertThat(capturedState.get()).isSameAs(state.state());
    }

    @Test
    void matchesAndExistsHelpersReturnMatchedObjects() {
        var state = workflowState(List.of("createUser", "activateUser2"),
                                  Map.of("id", "2"),
                                  step("createUser", StepStatus.COMPLETED),
                                  step("activateUser2", StepStatus.STARTED));

        assertThat(state.stepExists().stepName()).isEqualTo("createUser");
        assertThat(state.stepMatches(step -> step.stepName().equals("activateUser2")).stepName()).isEqualTo("activateUser2");
        assertThat(state.payloadExists()).isEqualTo(Map.of("id", "2"));
        assertThat(state.payloadMatches(payload -> payload.containsKey("id"))).isEqualTo(Map.of("id", "2"));
        assertThat(state.workflowStateExists()).isSameAs(state.state());
        assertThat(state.workflowStateMatches(workflowState -> workflowState.payload().containsKey("id")))
                .isSameAs(state.state());
    }

    @Test
    void waitingInReportsExpectedAndActualWaitingStep() {
        var failureHandler = AssertionFailureHandler.handler(false);

        WorkflowState state = mock(WorkflowState.class);
        MutableWorkflowEngineTestingState testingState = new MutableWorkflowEngineTestingState();
        testingState.setHistory(new WorkflowHistory("wf-1", state));
        when(state.workflowStepNames()).thenReturn(List.of("createUser", "activateUser", "activateUser2"));
        when(state.getStep("createUser")).thenReturn(WorkflowStep.completed("createUser", Map.of(), Instant.EPOCH, null));
        when(state.getStep("activateUser")).thenReturn(WorkflowStep.completed("activateUser", Map.of(), Instant.EPOCH, null));
        when(state.getStep("activateUser2")).thenReturn(WorkflowStep.started("activateUser2", Map.of(), Instant.EPOCH, null));

        assertThatThrownBy(() -> testingState.waitingIn(failureHandler, "sendWelcomeEmail"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected current workflow execution to be waiting in step 'sendWelcomeEmail', but it was waiting in step 'activateUser2'.");
    }

    @Test
    void stepsPassedReportsActualStatus() {
        var failureHandler = AssertionFailureHandler.handler(false);

        WorkflowState state = mock(WorkflowState.class);
        MutableWorkflowEngineTestingState testingState = new MutableWorkflowEngineTestingState();
        testingState.setHistory(new WorkflowHistory("wf-1", state));
        when(state.containsStep("activateUser2")).thenReturn(true);
        when(state.getStep("activateUser2")).thenReturn(WorkflowStep.started("activateUser2", Map.of(), Instant.EPOCH, null));

        assertThatThrownBy(() -> testingState.stepsPassed(failureHandler, "activateUser2"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected current workflow execution to have passed step 'activateUser2', but that step is still STARTED.");
    }

    @Test
    void hasStepsInOrderReportsActualSteps() {
        var failureHandler = AssertionFailureHandler.handler(false);

        WorkflowState state = mock(WorkflowState.class);
        MutableWorkflowEngineTestingState testingState = new MutableWorkflowEngineTestingState();
        testingState.setHistory(new WorkflowHistory("wf-1", state));
        when(state.workflowStepNames()).thenReturn(List.of("createUser", "activateUser", "activateUser2"));

        assertThatThrownBy(() -> testingState.hasStepsInOrder(failureHandler,
                "createUser", "activateUser", "sendWelcomeEmail"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow state to contain exactly [createUser, activateUser, sendWelcomeEmail] in order, but actual steps are [createUser, activateUser, activateUser2].");
    }

    @Test
    void hasStepsReportsKnownSteps() {
        var failureHandler = AssertionFailureHandler.handler(false);
        var state = workflowState(List.of("createUser", "activateUser2"),
                                  step("createUser", StepStatus.COMPLETED),
                                  step("activateUser2", StepStatus.STARTED));

        assertThatThrownBy(() -> state.hasSteps(failureHandler, "sendWelcomeEmail"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow state to contain step 'sendWelcomeEmail', but known steps are [createUser, activateUser2].")
                .hasNoCause();
    }

    @Test
    void hasStepsInAnyOrderReportsActualSteps() {
        var failureHandler = AssertionFailureHandler.handler(false);
        var state = workflowState(List.of("createUser", "activateUser", "activateUser2"),
                                  step("createUser", StepStatus.COMPLETED),
                                  step("activateUser", StepStatus.COMPLETED),
                                  step("activateUser2", StepStatus.STARTED));

        assertThatThrownBy(() -> state.hasStepsInAnyOrder(failureHandler,
                "createUser", "activateUser", "sendWelcomeEmail"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow state to contain exactly [createUser, activateUser, sendWelcomeEmail] in any order, but actual steps are [createUser, activateUser, activateUser2].")
                .hasNoCause();
    }

    @Test
    void doesNotContainStepReportsUnexpectedSteps() {
        var state = workflowState(List.of("createUser", "activateUser2"),
                                  step("createUser", StepStatus.COMPLETED),
                                  step("activateUser2", StepStatus.STARTED));

        assertThatThrownBy(() -> state.noStep("activateUser2"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow state not to contain [activateUser2], but actual steps are [createUser, activateUser2].")
                .hasNoCause();
    }

    @Test
    void stepMatchesReportsActualStepStatuses() {
        var failureHandler = AssertionFailureHandler.handler(false);
        var state = workflowState(List.of("createUser", "activateUser2"),
                                  step("createUser", StepStatus.COMPLETED),
                                  step("activateUser2", StepStatus.STARTED));

        assertThatThrownBy(() -> state.stepMatches(failureHandler, step -> step.status() == StepStatus.FAILED))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow state to contain a matching step, but actual steps are [createUser=COMPLETED, activateUser2=STARTED].")
                .hasNoCause();
    }

    @Test
    void payloadMatchesReportsActualPayload() {
        var failureHandler = AssertionFailureHandler.handler(false);
        var state = workflowState(List.of("createUser"),
                                  Map.of("id", "2"),
                                  step("createUser", StepStatus.COMPLETED));

        assertThatThrownBy(() -> state.payloadMatches(failureHandler, payload -> payload.containsKey("email")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow payload to match the given predicate, but actual payload is {id=2}.")
                .hasNoCause();
    }

    @Test
    void workflowStateMatchesReportsActualState() {
        var failureHandler = AssertionFailureHandler.handler(false);
        var state = workflowState(List.of("createUser", "activateUser2"),
                                  Map.of("id", "2"),
                                  step("createUser", StepStatus.COMPLETED),
                                  step("activateUser2", StepStatus.STARTED));

        assertThatThrownBy(() -> state.workflowStateMatches(failureHandler, workflowState -> workflowState.workflowStepNames().size() == 1))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow state to match the given predicate, but actual state is steps=[createUser=COMPLETED, activateUser2=STARTED], payload={id=2}.")
                .hasNoCause();
    }

    private WorkflowEngineTestingState workflowState(List<String> stepNames, WorkflowStep... steps) {
        return workflowState(stepNames, Map.of(), steps);
    }

    private WorkflowEngineTestingState workflowState(List<String> stepNames,
                                                     Map<String, @Nullable Object> payload,
                                                     WorkflowStep... steps) {
        WorkflowState state = mock(WorkflowState.class);
        when(state.workflowStepNames()).thenReturn(stepNames);
        when(state.payload()).thenReturn(payload);
        for (WorkflowStep step : steps) {
            when(state.containsStep(step.stepName())).thenReturn(true);
            when(state.getStep(step.stepName())).thenReturn(step);
        }
        MutableWorkflowEngineTestingState testingState = new MutableWorkflowEngineTestingState();
        testingState.setHistory(new WorkflowHistory("wf-1", state));
        return testingState;
    }

    private WorkflowStep step(String stepName, StepStatus stepStatus) {
        return switch (stepStatus) {
            case STARTED -> WorkflowStep.started(stepName, Map.of(), Instant.EPOCH, null);
            case COMPLETED -> WorkflowStep.completed(stepName, Map.of(), Instant.EPOCH, null);
            case FAILED -> WorkflowStep.failed(stepName, new RuntimeException("boom"), Instant.EPOCH, null);
            case TIMED_OUT -> WorkflowStep.timedOut(stepName, Map.of(), Instant.EPOCH, null);
            case CANCELLED -> WorkflowStep.cancelled(stepName, Instant.EPOCH, null);
            case RETRYING, RETRY_STARTED -> throw new IllegalArgumentException("Retrying is not used in these tests");
        };
    }
}
