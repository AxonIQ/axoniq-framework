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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import jakarta.annotation.Nonnull;
import org.awaitility.core.ThrowingRunnable;
import org.axonframework.common.annotation.Internal;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Default implementation of {@link WorkflowTestDriver}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class DefaultWorkflowTestDriver implements WorkflowTestDriver {

    private final WorkflowTestServices workflowTestServices;
    private final MutableWorkflowEngineTestingState testingState;
    private final AssertionFailureHandler assertionFailureHandler;
    private final boolean steppingMode;

    /**
     * Constructs the test driver.
     *
     * @param workflowTestServices    services for testing workflow runtime
     * @param testingState            state of the testing workflow runtime
     * @param assertionFailureHandler handler responsible for handling assertion failures
     * @param steppingMode            whether the test driver is in stepping mode
     */
    DefaultWorkflowTestDriver(@Nonnull WorkflowTestServices workflowTestServices,
                              @Nonnull MutableWorkflowEngineTestingState testingState,
                              @Nonnull AssertionFailureHandler assertionFailureHandler,
                              boolean steppingMode) {
        this.workflowTestServices =
                Objects.requireNonNull(workflowTestServices, "Workflow test services must not be null");
        this.testingState = Objects.requireNonNull(testingState, "Testing state must not be null");
        this.assertionFailureHandler = Objects.requireNonNull(assertionFailureHandler,
                                                              "Assertion failure handler must not be null");
        this.steppingMode = steppingMode;
    }

    @Override
    @Nonnull
    public WorkflowTestServices workflowTestServices() {
        return workflowTestServices;
    }

    @Override
    @Nonnull
    public WorkflowEngineTestingState testingState() {
        return testingState;
    }

    @Override
    @Nonnull
    public AssertionFailureHandler assertionFailureHandler() {
        return assertionFailureHandler;
    }

    @Override
    public boolean steppingMode() {
        return steppingMode;
    }

    @Override
    public WorkflowExecution executionExists() {
        return executionMatches(e -> true);
    }

    @Override
    public WorkflowExecution executionMatches(@Nonnull Predicate<WorkflowExecution> predicate) {
        Objects.requireNonNull(predicate, "Predicate must not be null");
        awaitEventually("Expected an active workflow execution matching the given predicate to appear",
                        () -> {
                            var executions = this.workflowTestServices.workflowEngine().workflowExecutions();
                            if (executions.stream().noneMatch(predicate)) {
                                throw new AssertionError(
                                        "Expected an active workflow execution matching the given predicate, but active executions are %s."
                                                .formatted(executions.stream().map(WorkflowExecution::workflowId)
                                                                     .toList())
                                );
                            }
                        }
        );

        var executions = this.workflowTestServices.workflowEngine().workflowExecutions().stream().filter(predicate)
                                                  .toList();
        this.mutableTestingState().setExecution(executions.getFirst());
        this.historyMatches(h -> h.workflowId().equals(this.testingState.execution().workflowId()));
        return this.testingState.execution();
    }

    @Override
    public void executionSatisfies(@Nonnull Consumer<WorkflowExecution> executionConsumer) {
        Objects.requireNonNull(executionConsumer, "Execution consumer must not be null");
        executionConsumer.accept(executionExists());
    }

    @Override
    public void noExecution() {
        awaitEventually("Expected all active workflow executions to disappear",
                        () -> {
                            var executions = this.workflowTestServices().workflowEngine().workflowExecutions();
                            if (!executions.isEmpty()) {
                                throw new AssertionError(
                                        "Expected no active workflow executions, but active executions are %s."
                                                .formatted(executions.stream().map(WorkflowExecution::workflowId)
                                                                     .toList())
                                );
                            }
                        });
        this.mutableTestingState().setExecution(null);
    }

    @Override
    public WorkflowHistory historyMatches(@Nonnull Predicate<WorkflowHistory> predicate) {
        Objects.requireNonNull(predicate, "Predicate must not be null");
        awaitEventually("Expected a workflow history entry matching the given predicate to appear",
                        () -> {
                            var histories = this.workflowTestServices.workflowHistoryRepository().findAll();
                            if (histories.stream().noneMatch(predicate)) {
                                throw new AssertionError(
                                        "Expected a workflow history entry matching the given predicate, but recorded histories are %s."
                                                .formatted(histories.stream().map(WorkflowHistory::workflowId).toList())
                                );
                            }
                        }
        );

        var histories = this.workflowTestServices.workflowHistoryRepository().findAll().stream()
                                                 .filter(predicate).toList();
        this.mutableTestingState().setHistory(histories.getFirst());
        return this.testingState.history();
    }

    @Override
    public WorkflowHistory historyExists() {
        return historyMatches(e -> true);
    }

    @Override
    public void historySatisfies(@Nonnull Consumer<WorkflowHistory> historyConsumer) {
        Objects.requireNonNull(historyConsumer, "History consumer must not be null");
        historyConsumer.accept(historyExists());
    }

    @Override
    public void noHistory() {
        var histories = workflowTestServices().workflowHistoryRepository().findAll();
        if (!histories.isEmpty()) {
            throw new AssertionError(
                    "Expected no workflow history, but recorded histories are %s."
                            .formatted(histories.stream().map(WorkflowHistory::workflowId).toList())
            );
        }
        this.mutableTestingState().setHistory(null);
    }

    @Override
    public void publishEvent(@Nonnull Object event) {
        Objects.requireNonNull(event, "Event must not be null");
        this.workflowTestServices.eventPublisher().publish(event).join();
    }

    @Override
    public void timePasses(@Nonnull Duration duration) {
        Objects.requireNonNull(duration, "Duration must not be null");
        if (!steppingMode) {
            throw new IllegalStateException(
                    "Time control is not supported in live mode, consider starting in stepper mode.");
        }
        var now = this.workflowTestServices.clock()
                                           .orElseThrow(() -> new IllegalStateException("Could not retrieve clock"))
                                           .advanceBy(duration);
        this.workflowTestServices.timeoutScheduler()
                                 .orElseThrow(() -> new IllegalStateException("Could not retrieve scheduler"))
                                 .runDueTasks(now);
    }

    @Override
    public void executeStep(@Nonnull String stepName) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        if (!steppingMode) {
            throw new IllegalStateException(
                    "Stepping of execution is not supported in live mode, consider starting in stepper mode.");
        }
        this.workflowTestServices.executeStepActionResolver()
                                 .orElseThrow(() -> new IllegalStateException("Could not retrieve step action resolver"))
                                 .applyOriginalAction(stepName);
        this.historyExists();
        awaitEventually("Expected step '%s' to reach a terminal state after running the workflow-defined action"
                                .formatted(stepName),
                        () -> assertStepReachedTerminalState(
                                stepName,
                                "the workflow-defined action"
                        ));
    }

    @Override
    public void executeStep(@Nonnull String stepName, @Nonnull PayloadProcessor payloadProcessor) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        Objects.requireNonNull(payloadProcessor, "Payload processor must not be null");
        if (!steppingMode) {
            throw new IllegalStateException(
                    "Stepping of execution is not supported in live mode, consider starting in stepper mode.");
        }
        this.workflowTestServices.executeStepActionResolver()
                                 .orElseThrow(() -> new IllegalStateException("Could not retrieve step action resolver"))
                                 .applyAction(stepName, payloadProcessor);
        awaitEventually("Expected step '%s' to reach a terminal state after running the supplied test action"
                                .formatted(stepName),
                        () -> assertStepReachedTerminalState(
                                stepName,
                                "the supplied test action"
                        ));
    }

    @Override
    public void executeStepFailing(@Nonnull String stepName, @Nonnull StepFailedException exception) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        Objects.requireNonNull(exception, "Exception must not be null");
        if (!steppingMode) {
            throw new IllegalStateException(
                    "Stepping of execution is not supported in live mode, consider starting in stepper mode.");
        }
        executeStep(stepName, (payload, context) -> {
            throw exception;
        });
    }

    @Override
    public void shutdown() {
        this.workflowTestServices.shutdown();
    }

    @Override
    public void awaitEventually(@Nonnull String description,
                                @Nonnull ThrowingRunnable assertion) {
        MutableWorkflowEngineTestingState.awaitEventually(this.assertionFailureHandler, description, assertion);
    }

    MutableWorkflowEngineTestingState mutableTestingState() {
        return testingState;
    }

    private void assertStepReachedTerminalState(@Nonnull String stepName, @Nonnull String actionDescription) {
        var state = this.testingState.state();
        if (!state.containsStep(stepName)) {
            throw new AssertionError(
                    "Could not execute step '%s' using %s because that step was not started. Current workflow execution is waiting in %s. Finished steps: %s."
                            .formatted(stepName,
                                       actionDescription,
                                       formatWaitingSteps(state),
                                       formatFinishedSteps(state))
            );
        }

        var stepStatus = state.getStep(stepName).status();
        if (!stepStatus.isTerminal()) {
            throw new AssertionError(
                    "Execution of step '%s' using %s did not finish. Current step status is %s."
                            .formatted(stepName, actionDescription, stepStatus)
            );
        }
    }

    @Nonnull
    private static String formatWaitingSteps(@Nonnull WorkflowState state) {
        var waitingSteps = state.workflowStepNames().stream()
                                .filter(name -> !state.getStep(name).status().isTerminal())
                                .toList();
        if (waitingSteps.isEmpty()) {
            return "no step";
        }
        return waitingSteps.size() == 1
                ? "step '%s'".formatted(waitingSteps.getFirst())
                : "steps %s".formatted(waitingSteps);
    }

    @Nonnull
    private static String formatFinishedSteps(@Nonnull WorkflowState state) {
        var finishedSteps = state.workflowStepNames().stream()
                                 .filter(name -> state.getStep(name).status().isTerminal())
                                 .toList();
        if (finishedSteps.isEmpty()) {
            return "none";
        }
        return finishedSteps.toString();
    }
}
