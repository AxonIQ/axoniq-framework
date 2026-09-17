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

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.framework.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.TestClock;
import io.axoniq.framework.workflow.runtime.test.utils.TestEventPublisher;
import org.axonframework.common.configuration.AxonConfiguration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestDriver.stepNames;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowTestDriver}.
 *
 * @author Simon Zambrovski
 */
class WorkflowTestDriverTest {

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
    void stepNamesIncludesMandatoryAndAdditionalNames() {
        assertThat(stepNames("reserve", "charge", "confirm"))
                .containsExactly("reserve", "charge", "confirm");
    }

    @Test
    void stepNamesRejectsNullMandatoryName() {
        assertThatThrownBy(() -> stepNames(null, "charge"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Step name must not be null");
    }

    @Test
    void executionExistsReportsTimeoutContext() {
        var driver = driverWith(List.of(), List.of(), false, false, mock(ManualExecuteStepActionResolver.class));

        assertThatThrownBy(driver::executionExists)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected an active workflow execution matching the given predicate, but active executions are [].")
                .hasNoCause();
    }

    @Test
    void noExecutionReportsTimeoutContext() {
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn("wf-42");
        var driver = driverWith(List.of(execution), List.of(), false, false, mock(ManualExecuteStepActionResolver.class));

        assertThatThrownBy(driver::noExecution)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected no active workflow executions, but active executions are [wf-42].")
                .hasNoCause();
    }

    @Test
    void historyExistsReportsTimeoutContext() {
        var driver = driverWith(List.of(), List.of(), false, false, mock(ManualExecuteStepActionResolver.class));

        assertThatThrownBy(driver::historyExists)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected a workflow history entry matching the given predicate, but recorded histories are [].")
                .hasNoCause();
    }

    @Test
    void noHistoryReportsRecordedHistories() {
        var history = new WorkflowHistory("wf-42", mock(WorkflowState.class));
        var driver = driverWith(List.of(), List.of(history), false, false, mock(ManualExecuteStepActionResolver.class));

        assertThatThrownBy(driver::noHistory)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected no workflow history, but recorded histories are [wf-42].")
                .hasNoCause();
    }

    @Test
    void executionSatisfiesProvidesSelectedExecution() {
        var state = mock(WorkflowState.class);
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn("wf-42");
        when(execution.state()).thenReturn(state);
        var history = new WorkflowHistory("wf-42", state);
        var driver = driverWith(List.of(execution), List.of(history), false, false, mock(ManualExecuteStepActionResolver.class));
        AtomicReference<WorkflowExecution> captured = new AtomicReference<>();

        driver.executionSatisfies(captured::set);

        assertThat(captured.get()).isSameAs(execution);
        assertThat(driver.testingState().execution()).isSameAs(execution);
        assertThat(driver.testingState().history()).isSameAs(history);
    }

    @Test
    void executionExistsReturnsSelectedExecution() {
        var state = mock(WorkflowState.class);
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn("wf-42");
        when(execution.state()).thenReturn(state);
        var history = new WorkflowHistory("wf-42", state);
        var driver = driverWith(List.of(execution), List.of(history), false, false, mock(ManualExecuteStepActionResolver.class));

        var selectedExecution = driver.executionExists();

        assertThat(selectedExecution).isSameAs(execution);
        assertThat(driver.testingState().execution()).isSameAs(execution);
        assertThat(driver.testingState().history()).isSameAs(history);
    }

    @Test
    void executionMatchesSelectsMatchingExecution() {
        var state = mock(WorkflowState.class);
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn("wf-42");
        when(execution.state()).thenReturn(state);
        var history = new WorkflowHistory("wf-42", state);
        var driver = driverWith(List.of(execution), List.of(history), false, false, mock(ManualExecuteStepActionResolver.class));

        var selectedExecution = driver.executionMatches(workflowExecution -> workflowExecution.workflowId().equals("wf-42"));

        assertThat(selectedExecution).isSameAs(execution);
        assertThat(driver.testingState().execution()).isSameAs(execution);
        assertThat(driver.testingState().history()).isSameAs(history);
    }

    @Test
    void historySatisfiesProvidesSelectedHistory() {
        var history = new WorkflowHistory("wf-42", mock(WorkflowState.class));
        var driver = driverWith(List.of(), List.of(history), false, false, mock(ManualExecuteStepActionResolver.class));
        AtomicReference<WorkflowHistory> captured = new AtomicReference<>();

        driver.historySatisfies(captured::set);

        assertThat(captured.get()).isSameAs(history);
        assertThat(driver.testingState().history()).isSameAs(history);
    }

    @Test
    void historyExistsReturnsSelectedHistory() {
        var history = new WorkflowHistory("wf-42", mock(WorkflowState.class));
        var driver = driverWith(List.of(), List.of(history), false, false, mock(ManualExecuteStepActionResolver.class));

        var selectedHistory = driver.historyExists();

        assertThat(selectedHistory).isSameAs(history);
        assertThat(driver.testingState().history()).isSameAs(history);
    }

    @Test
    void historyMatchesSelectsMatchingHistory() {
        var history = new WorkflowHistory("wf-42", mock(WorkflowState.class));
        var driver = driverWith(List.of(), List.of(history), false, false, mock(ManualExecuteStepActionResolver.class));

        var selectedHistory = driver.historyMatches(workflowHistory -> workflowHistory.workflowId().equals("wf-42"));

        assertThat(selectedHistory).isSameAs(history);
        assertThat(driver.testingState().history()).isSameAs(history);
    }

    @Test
    void executeReportsStepNameAndActionType() {
        var state = mock(WorkflowState.class);
        when(state.workflowStepNames()).thenReturn(List.of("approve"));
        when(state.containsStep("approve")).thenReturn(true);
        when(state.getStep("approve")).thenReturn(WorkflowStep.started("approve", Map.of(), Instant.EPOCH, null));

        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn("wf-1");
        when(execution.state()).thenReturn(state);
        var history = new WorkflowHistory("wf-1", state);
        var resolver = mock(ManualExecuteStepActionResolver.class);
        var driver = driverWith(List.of(execution), List.of(history), true, true, resolver);

        assertThatThrownBy(() -> driver.executeStep("approve"))
                .isInstanceOf(WorkflowTestFixtureException.class)
                .hasMessage("Execution of step 'approve' using the workflow-defined action did not finish. Current step status is STARTED.")
                .hasNoCause();

        verify(resolver).applyOriginalAction("approve");
    }

    @Test
    void executeWithResultReportsWhenStepWasNotStarted() {
        var state = mock(WorkflowState.class);
        when(state.workflowStepNames()).thenReturn(List.of("createUser", "activateUser", "activateUser2"));
        when(state.containsStep("sendWelcomeEmail")).thenReturn(false);
        when(state.getStep("createUser")).thenReturn(WorkflowStep.completed("createUser", Map.of(), Instant.EPOCH, null));
        when(state.getStep("activateUser")).thenReturn(WorkflowStep.completed("activateUser", Map.of(), Instant.EPOCH, null));
        when(state.getStep("activateUser2")).thenReturn(WorkflowStep.started("activateUser2", Map.of(), Instant.EPOCH, null));

        var resolver = mock(ManualExecuteStepActionResolver.class);
        var driver = driverWith(state, true, true, resolver);

        assertThatThrownBy(() -> driver.executeStep("sendWelcomeEmail", (payload, context) -> Map.of()))
                .isInstanceOf(WorkflowTestFixtureException.class)
                .hasMessage("Could not execute step 'sendWelcomeEmail' using the supplied test action because that step was not started. Current workflow execution is waiting in step 'activateUser2'. Finished steps: [createUser, activateUser].")
                .hasNoCause();

        verify(resolver).applyAction(eq("sendWelcomeEmail"), any());
    }

    @Test
    void executeWithResultReportsStepNameAndActionType() {
        var state = mock(WorkflowState.class);
        when(state.workflowStepNames()).thenReturn(List.of("approve"));
        when(state.containsStep("approve")).thenReturn(true);
        when(state.getStep("approve")).thenReturn(WorkflowStep.started("approve", Map.of(), Instant.EPOCH, null));

        var resolver = mock(ManualExecuteStepActionResolver.class);
        var driver = driverWith(state, true, true, resolver);

        assertThatThrownBy(() -> driver.executeStep("approve", (payload, context) -> Map.of()))
                .isInstanceOf(WorkflowTestFixtureException.class)
                .hasMessage("Execution of step 'approve' using the supplied test action did not finish. Current step status is STARTED.")
                .hasNoCause();

        verify(resolver).applyAction(eq("approve"), any());
    }

    private WorkflowTestDriver driverWith(List<WorkflowExecution> executions,
                                          List<WorkflowHistory> histories,
                                          boolean steppingMode) {
        return driverWith(executions, histories, steppingMode, false, mock(ManualExecuteStepActionResolver.class));
    }

    private WorkflowTestDriver driverWith(List<WorkflowExecution> executions,
                                          List<WorkflowHistory> histories,
                                          boolean steppingMode,
                                          boolean throwFixtureException,
                                          ManualExecuteStepActionResolver resolver) {
        var workflowEngine = mock(WorkflowEngine.class);
        when(workflowEngine.workflowExecutions()).thenReturn(Set.copyOf(executions));

        var historyRepository = mock(MutableWorkflowHistoryRepository.class);
        when(historyRepository.findAll()).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(histories));

        var configuration = mock(AxonConfiguration.class);
        when(configuration.getComponent(WorkflowEngine.class)).thenReturn(workflowEngine);
        when(configuration.getComponent(DelayedPublisher.class)).thenReturn(mock(DelayedPublisher.class));
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(mock(TestEventPublisher.class));
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(
                mock(WorkflowConfigurationRegistry.class));
        when(configuration.getComponent(MutableWorkflowHistoryRepository.class)).thenReturn(historyRepository);
        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(Optional.empty());
        when(configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)).thenReturn(Optional.of(resolver));
        when(configuration.getOptionalComponent(ManualWorkflowScheduler.class)).thenReturn(Optional.empty());

        var services = WorkflowTestServices.from(configuration);

        return new DefaultWorkflowTestDriver(services,
                                             new MutableWorkflowEngineTestingState(),
                                             AssertionFailureHandler.handler(throwFixtureException),
                                             steppingMode);
    }

    private WorkflowTestDriver driverWith(WorkflowState state,
                                          boolean steppingMode,
                                          boolean throwFixtureException,
                                          ManualExecuteStepActionResolver resolver) {
        var historyRepository = mock(MutableWorkflowHistoryRepository.class);
        var configuration = mock(AxonConfiguration.class);
        when(configuration.getComponent(WorkflowEngine.class)).thenReturn(mock(WorkflowEngine.class));
        when(configuration.getComponent(DelayedPublisher.class)).thenReturn(mock(DelayedPublisher.class));
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(mock(TestEventPublisher.class));
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(
                mock(WorkflowConfigurationRegistry.class));
        when(configuration.getComponent(MutableWorkflowHistoryRepository.class)).thenReturn(historyRepository);
        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(Optional.empty());
        when(configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)).thenReturn(Optional.of(resolver));
        when(configuration.getOptionalComponent(ManualWorkflowScheduler.class)).thenReturn(Optional.empty());

        var services = WorkflowTestServices.from(configuration);

        var testingState = new MutableWorkflowEngineTestingState();
        testingState.setHistory(new WorkflowHistory("wf-1", state));
        return new DefaultWorkflowTestDriver(services,
                                             testingState,
                                             AssertionFailureHandler.handler(throwFixtureException),
                                             steppingMode);
    }
}
