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
package io.axoniq.workflow.runtime.test.fixture;

import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.workflow.runtime.test.utils.TestClock;
import io.axoniq.workflow.runtime.test.utils.TestEventPublisher;
import org.awaitility.Awaitility;
import org.axonframework.common.configuration.AxonConfiguration;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link Then}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class ThenTest {

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
    void workflowNotFinishedReportsTerminalStatus() {
        var phase = new Then.Phase();
        var state = mock(WorkflowState.class);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);
        phase.testDriver = testDriverWith(state, List.of());

        assertThatThrownBy(phase::workflowNotFinished)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected current workflow to remain non-terminal, but workflow status is COMPLETED.")
                .hasNoCause();
    }

    @Test
    void andReturnsFixture() {
        var phase = new Then.Phase();
        var fixture = mock(WorkflowTestFixture.class);
        phase.fixture = fixture;

        assertThat(phase.and()).isSameAs(fixture);
    }

    @Test
    void waitingInReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("activateUser2"),
                              Map.of(),
                              step("activateUser2", StepStatus.STARTED))
        );

        assertThat(phase.waitingIn("activateUser2")).isSameAs(phase);
    }

    @Test
    void stepsPassedReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("createUser"),
                              Map.of(),
                              step("createUser", StepStatus.COMPLETED))
        );

        assertThat(phase.stepsPassed("createUser")).isSameAs(phase);
    }

    @Test
    void hasStepsReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("createUser", "activateUser2"),
                              Map.of(),
                              step("createUser", StepStatus.COMPLETED),
                              step("activateUser2", StepStatus.STARTED))
        );

        assertThat(phase.hasSteps("createUser", "activateUser2")).isSameAs(phase);
    }

    @Test
    void hasStepsInAnyOrderReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("createUser", "activateUser2"),
                              Map.of(),
                              step("createUser", StepStatus.COMPLETED),
                              step("activateUser2", StepStatus.STARTED))
        );

        assertThat(phase.hasStepsInAnyOrder("activateUser2", "createUser")).isSameAs(phase);
    }

    @Test
    void noStepReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("createUser"),
                              Map.of(),
                              step("createUser", StepStatus.COMPLETED))
        );

        assertThat(phase.noStep("activateUser2")).isSameAs(phase);
    }

    @Test
    void workflowFinishedReportsRecordedStatuses() {
        var phase = new Then.Phase();
        var state = mock(WorkflowState.class);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.FAILED);
        phase.testDriver = testDriverWith(null, List.of(new WorkflowHistory("wf-1", state)));

        assertThatThrownBy(() -> phase.workflowFinished(WorkflowStatus.COMPLETED))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected workflow to finish with status COMPLETED, but recorded workflow history statuses are [FAILED].")
                .hasNoCause();
    }

    @Test
    void workflowFinishedReturnsSelf() {
        var state = mock(WorkflowState.class);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);
        var history = new WorkflowHistory("wf-1", state);

        var phase = new Then.Phase();
        phase.testDriver = testDriverWith(null, List.of(history));

        assertThat(phase.workflowFinished(WorkflowStatus.COMPLETED)).isSameAs(phase);
        assertThat(phase.testDriver.testingState().history()).isSameAs(history);
    }

    @Test
    void payloadSatisfiesInvokesConsumerWithPayload() {
        var phase = phaseWithState(
                workflowState(List.of("createUser"),
                              Map.of("id", "2"),
                              step("createUser", StepStatus.COMPLETED))
        );
        AtomicReference<Map<String, Object>> captured = new AtomicReference<>();

        assertThat(phase.payloadSatisfies(captured::set)).isSameAs(phase);
        assertThat(captured.get()).isEqualTo(Map.of("id", "2"));
    }

    @Test
    void payloadMatchesReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("createUser"),
                              Map.of("id", "2"),
                              step("createUser", StepStatus.COMPLETED))
        );

        assertThat(phase.payloadMatches(payload -> payload.containsKey("id"))).isSameAs(phase);
    }

    @Test
    void payloadEqualsReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("createUser"),
                              Map.of("id", "2"),
                              step("createUser", StepStatus.COMPLETED))
        );

        assertThat(phase.payloadEquals(Map.of("id", "2"))).isSameAs(phase);
    }

    @Test
    void payloadContainsReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("createUser"),
                              Map.of("id", "2", "email", "piggy@muppets.biz"),
                              step("createUser", StepStatus.COMPLETED))
        );

        assertThat(phase.payloadContains(Map.of("id", "2"))).isSameAs(phase);
    }

    @Test
    void workflowStateSatisfiesInvokesConsumerWithState() {
        var state = mock(WorkflowState.class);
        when(state.payload()).thenReturn(Map.of("id", "2"));
        var phase = phaseWithState(state);
        AtomicReference<WorkflowState> captured = new AtomicReference<>();

        assertThat(phase.workflowStateSatisfies(captured::set)).isSameAs(phase);
        assertThat(captured.get()).isSameAs(state);
    }

    @Test
    void stepReturnsSelf() {
        var phase = phaseWithState(
                workflowState(List.of("activateUser2"),
                              Map.of(),
                              step("activateUser2", StepStatus.STARTED))
        );

        assertThat(phase.step("activateUser2", StepStatus.STARTED)).isSameAs(phase);
    }

    private WorkflowTestDriver testDriverWith(WorkflowState selectedState, List<WorkflowHistory> histories) {
        var historyRepository = mock(MutableWorkflowHistoryRepository.class);
        when(historyRepository.findAll()).thenReturn(histories);

        var workflowEngine = mock(WorkflowEngine.class);
        when(workflowEngine.workflowExecutions()).thenReturn(Set.of());

        var configuration = mock(AxonConfiguration.class);
        when(configuration.getComponent(WorkflowEngine.class)).thenReturn(workflowEngine);
        when(configuration.getComponent(DelayedPublisher.class)).thenReturn(mock(DelayedPublisher.class));
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(mock(TestEventPublisher.class));
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(
                mock(WorkflowConfigurationRegistry.class));
        when(configuration.getComponent(MutableWorkflowHistoryRepository.class)).thenReturn(historyRepository);
        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(Optional.empty());
        when(configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)).thenReturn(Optional.empty());
        when(configuration.getOptionalComponent(ManualWorkflowScheduler.class)).thenReturn(Optional.empty());

        var services = WorkflowTestServices.from(
                configuration
        );

        var testingState = new MutableWorkflowEngineTestingState();
        if (selectedState != null) {
            testingState.setHistory(new WorkflowHistory("wf-selected", selectedState));
        }

        return new DefaultWorkflowTestDriver(services,
                                             testingState,
                                             AssertionFailureHandler.handler(false),
                                             false);
    }

    private Then.Phase phaseWithState(WorkflowState state) {
        var phase = new Then.Phase();
        phase.testDriver = testDriverWith(state, List.of());
        return phase;
    }

    private WorkflowState workflowState(List<String> stepNames,
                                        Map<String, Object> payload,
                                        WorkflowStep... steps) {
        WorkflowState state = mock(WorkflowState.class);
        when(state.workflowStepNames()).thenReturn(stepNames);
        when(state.payload()).thenReturn(payload);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        for (WorkflowStep step : steps) {
            when(state.containsStep(step.stepName())).thenReturn(true);
            when(state.getStep(step.stepName())).thenReturn(step);
        }
        return state;
    }

    private WorkflowStep step(String stepName, StepStatus stepStatus) {
        return switch (stepStatus) {
            case STARTED -> WorkflowStep.started(stepName, Map.of(), Instant.EPOCH, null);
            case COMPLETED -> WorkflowStep.completed(stepName, Map.of(), Instant.EPOCH, null);
            case FAILED -> WorkflowStep.failed(stepName, new RuntimeException("boom"), Instant.EPOCH, null);
            case TIMED_OUT -> WorkflowStep.timedOut(stepName, Map.of(), Instant.EPOCH, null);
            case CANCELLED -> WorkflowStep.cancelled(stepName, Instant.EPOCH, null);
            case RETRYING -> throw new IllegalArgumentException("Retrying is not used in these tests");
        };
    }
}
