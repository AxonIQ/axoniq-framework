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

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.core.ThrowingRunnable;
import org.axonframework.common.annotation.Internal;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.test.fixture.WorkflowTestDriver.stepNames;
import static org.awaitility.Awaitility.await;

/**
 * Mutable implementation of {@link WorkflowEngineTestingState} holding at most one execution and one history.
 *
 * @author Simon Zambrovski
 * @since 0.2.0
 */
@Internal
class MutableWorkflowEngineTestingState implements WorkflowEngineTestingState {

    private WorkflowExecution execution;
    private WorkflowHistory history;

    @Override
    public WorkflowExecution execution() {
        return execution;
    }

    @Override
    public WorkflowHistory history() {
        return history;
    }

    @Override
    public WorkflowState state() {
        if (execution != null) {
            return execution().state();
        }
        if (history == null) {
            throw new AssertionError("No workflow execution or workflow history is currently selected.");
        }
        return history.state();
    }

    void setExecution(WorkflowExecution execution) {
        this.execution = execution;
    }

    void setHistory(WorkflowHistory history) {
        this.history = history;
    }

    @Override
    public void waitingIn(AssertionFailureHandler assertionFailureHandler,
                          String stepName,
                          String... stepNames) {
        var allNames = stepNames(stepName, stepNames);
        awaitEventually(assertionFailureHandler,
                        "Expected current workflow execution to be waiting in %s".formatted(formatExpectedSteps(allNames)),
                        () -> {
                            var actualWaitingSteps = waitingSteps();
                            for (String name : allNames) {
                                if (!actualWaitingSteps.contains(name)) {
                                    throw new AssertionError(
                                            "Expected current workflow execution to be waiting in %s, but it was waiting in %s."
                                                    .formatted(formatStepName(name),
                                                               formatActualWaitingSteps(actualWaitingSteps))
                                    );
                                }
                            }
                        });
    }

    @Override
    public void stepsPassed(AssertionFailureHandler assertionFailureHandler,
                            String stepName,
                            String... stepNames) {
        var allNames = stepNames(stepName, stepNames);
        awaitEventually(assertionFailureHandler,
                        "Expected current workflow execution to have passed %s".formatted(formatExpectedSteps(allNames)),
                        () -> {
                            for (String name : allNames) {
                                var state = state();
                                if (!state.containsStep(name)) {
                                    throw new AssertionError(
                                            "Expected current workflow execution to have passed %s, but known steps are %s."
                                                    .formatted(formatStepName(name), state.workflowStepNames())
                                    );
                                }
                                var stepStatus = state.getStep(name).status();
                                if (!stepStatus.isTerminal()) {
                                    throw new AssertionError(
                                            "Expected current workflow execution to have passed %s, but that step is still %s."
                                                    .formatted(formatStepName(name), stepStatus)
                                    );
                                }
                            }
                        });
    }

    @Override
    public void hasSteps(AssertionFailureHandler assertionFailureHandler,
                         String stepName,
                         String... stepNames) {
        var allNames = stepNames(stepName, stepNames);
        awaitEventually(assertionFailureHandler,
                        "Expected workflow state to contain %s".formatted(formatExpectedSteps(allNames)),
                        () -> {
                            for (String name : allNames) {
                                if (!state().containsStep(name)) {
                                    throw new AssertionError(
                                            "Expected workflow state to contain %s, but known steps are %s."
                                                    .formatted(formatStepName(name), state().workflowStepNames())
                                    );
                                }
                            }
                        });
    }

    @Override
    public void hasStepsInAnyOrder(AssertionFailureHandler assertionFailureHandler,
                                   String stepName,
                                   String... stepNames) {
        var allNames = stepNames(stepName, stepNames);
        awaitEventually(assertionFailureHandler,
                        "Expected workflow state to contain exactly %s in any order".formatted(formatExpectedSteps(
                                allNames)),
                        () -> {
                            var actual = state().workflowStepNames();
                            if (actual.size() != allNames.size() || !actual.containsAll(allNames)
                                    || !allNames.containsAll(actual)) {
                                throw new AssertionError(
                                        "Expected workflow state to contain exactly %s in any order, but actual steps are %s."
                                                .formatted(allNames, actual)
                                );
                            }
                        });
    }

    @Override
    public void hasStepsInOrder(AssertionFailureHandler assertionFailureHandler,
                                String stepName,
                                String... stepNames) {
        var allNames = stepNames(stepName, stepNames);
        awaitEventually(assertionFailureHandler,
                        "Expected workflow state to contain exactly %s in order".formatted(allNames),
                        () -> {
                            var actual = state().workflowStepNames();
                            if (!actual.equals(allNames)) {
                                throw new AssertionError(
                                        "Expected workflow state to contain exactly %s in order, but actual steps are %s."
                                                .formatted(allNames, actual)
                                );
                            }
                        });
    }

    @Override
    public void noStep(String stepName, String... stepNames) {
        var allNames = stepNames(stepName, stepNames);
        var actual = state().workflowStepNames();
        var unexpected = allNames.stream().filter(actual::contains).toList();
        if (!unexpected.isEmpty()) {
            throw new AssertionError(
                    "Expected workflow state not to contain %s, but actual steps are %s."
                            .formatted(unexpected, actual)
            );
        }
    }

    @Override
    public WorkflowStep stepMatches(AssertionFailureHandler assertionFailureHandler,
                                    Predicate<WorkflowStep> predicate) {
        awaitEventually(assertionFailureHandler,
                        "Expected workflow state to contain a step matching the given predicate",
                        () -> {
                            var steps = state().workflowStepNames().stream().map(sn -> state().getStep(sn)).toList();
                            if (steps.stream().noneMatch(predicate)) {
                                throw new AssertionError(
                                        "Expected workflow state to contain a matching step, but actual steps are %s."
                                                .formatted(formatStepStatuses(steps))
                                );
                            }
                        });
        return state().workflowStepNames().stream()
                      .map(sn -> state().getStep(sn))
                      .filter(predicate)
                      .findFirst()
                      .orElseThrow(() -> new AssertionError("No matching step was found after assertion."));
    }

    @Override
    public void stepSatisfies(Consumer<WorkflowStep> stepConsumer) {
        Objects.requireNonNull(stepConsumer, "Step consumer must not be null");
        stepConsumer.accept(stepExists());
    }

    @Override
    public void stepSatisfies(AssertionFailureHandler assertionFailureHandler,
                              Predicate<WorkflowStep> predicate,
                              Consumer<WorkflowStep> stepConsumer) {
        Objects.requireNonNull(stepConsumer, "Step consumer must not be null");
        stepConsumer.accept(stepMatches(assertionFailureHandler, predicate));
    }

    @Override
    public Map<String, @Nullable Object> payloadMatches(AssertionFailureHandler assertionFailureHandler,
                                              Predicate<Map<String, @Nullable Object>> predicate) {
        awaitEventually(assertionFailureHandler,
                        "Expected workflow payload to match the given predicate",
                        () -> {
                            var payload = state().payload();
                            if (!predicate.test(payload)) {
                                throw new AssertionError(
                                        "Expected workflow payload to match the given predicate, but actual payload is %s."
                                                .formatted(payload)
                                );
                            }
                        });
        return state().payload();
    }

    @Override
    public Map<String, @Nullable Object> payloadExists() {
        return payloadMatches(payload -> true);
    }

    @Override
    public void payloadSatisfies(Consumer<Map<String, @Nullable Object>> payloadConsumer) {
        Objects.requireNonNull(payloadConsumer, "Payload consumer must not be null");
        payloadConsumer.accept(payloadExists());
    }

    @Override
    public WorkflowState workflowStateMatches(AssertionFailureHandler assertionFailureHandler,
                                              Predicate<WorkflowState> predicate) {
        awaitEventually(assertionFailureHandler,
                        "Expected workflow state to match the given predicate",
                        () -> {
                            var workflowState = state();
                            if (!predicate.test(workflowState)) {
                                throw new AssertionError(
                                        "Expected workflow state to match the given predicate, but actual state is %s."
                                                .formatted(describeState(workflowState))
                                );
                            }
                        });
        return state();
    }

    @Override
    public void workflowStateSatisfies(Consumer<WorkflowState> workflowStateConsumer) {
        Objects.requireNonNull(workflowStateConsumer, "Workflow state consumer must not be null");
        workflowStateConsumer.accept(workflowStateExists());
    }

    /**
     * Waits until the given condition is met.
     *
     * @param description             description of the assertion
     * @param assertion               assertion to execute
     * @param assertionFailureHandler handler for assertion failures
     */
    static void awaitEventually(AssertionFailureHandler assertionFailureHandler,
                                String description,
                                ThrowingRunnable assertion) {
        AtomicReference<Throwable> lastFailure = new AtomicReference<>();
        try {
            await(description).untilAsserted(() -> {
                try {
                    assertion.run();
                    lastFailure.set(null);
                } catch (Throwable throwable) {
                    lastFailure.set(throwable);
                    throw throwable;
                }
            });
        } catch (ConditionTimeoutException e) {
            assertionFailureHandler.fail(lastAssertionMessage(description, e, lastFailure.get()), e);
        }
    }

    private List<String> waitingSteps() {
        return state().workflowStepNames().stream()
                      .filter(name -> !state().getStep(name).status().isTerminal())
                      .toList();
    }

    private static String lastAssertionMessage(String description,
                                               ConditionTimeoutException exception,
                                               Throwable lastFailure) {
        var cause = lastFailure != null ? lastFailure : exception.getCause();
        if (cause != null && cause.getMessage() != null && !cause.getMessage().isBlank()) {
            return cause.getMessage().trim();
        }
        return description;
    }

    private static String formatExpectedSteps(List<String> stepNames) {
        return stepNames.size() == 1
                ? formatStepName(stepNames.getFirst())
                : "steps %s".formatted(stepNames);
    }

    private static String formatActualWaitingSteps(List<String> stepNames) {
        if (stepNames.isEmpty()) {
            return "no step";
        }
        return stepNames.size() == 1
                ? formatStepName(stepNames.getFirst())
                : "steps %s".formatted(stepNames);
    }

    private static String formatStepName(String stepName) {
        return "step '%s'".formatted(stepName);
    }

    private static String formatStepStatuses(List<WorkflowStep> steps) {
        return steps.stream()
                    .map(step -> "%s=%s".formatted(step.stepName(), step.status()))
                    .toList()
                    .toString();
    }

    private static String describeState(WorkflowState state) {
        return "steps=%s, payload=%s".formatted(
                formatStepStatuses(state.workflowStepNames().stream().map(state::getStep).toList()),
                state.payload()
        );
    }
}
