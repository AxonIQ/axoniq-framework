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

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Shared selection and assertion state for workflow fixture tests.
 *
 * <p>This type exists to give the test driver and the BDD fixture phases a common view of "what workflow instance is
 * currently under test". Workflow tests often first select an active {@link WorkflowExecution} or a recorded
 * {@link WorkflowHistory} and then perform several assertions against that same workflow state. Without this shared
 * object, every assertion method would need to repeat selection logic or require the caller to pass workflow ids,
 * executions, or histories through every step of the fluent API.</p>
 *
 * <p>The design therefore separates two responsibilities:</p>
 * <ul>
 *     <li>{@link WorkflowTestDriver} discovers executions and history entries and updates the current selection</li>
 *     <li>{@code WorkflowEngineTestingState} keeps that selection and exposes reusable assertions over the selected
 *     {@link WorkflowState}</li>
 * </ul>
 *
 * <p>This separation keeps {@link GivenWhen}, {@link Then}, and the imperative driver APIs fluent. A test can select an
 * execution once and then assert waiting steps, passed steps, payload, or step status without restating which workflow
 * instance those assertions should inspect. The state also abstracts over active and completed workflows by exposing
 * {@link #state()}, which resolves to the live execution state when an execution is selected and otherwise falls back to
 * the selected history entry.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowEngineTestingState {

    /**
     * Returns the current workflow execution.
     *
     * @return the current workflow execution
     */
    WorkflowExecution execution();

    /**
     * Returns the current workflow history.
     *
     * @return the current workflow history
     */
    WorkflowHistory history();

    /**
     * Returns the historic workflow state.
     *
     * @return the historic workflow state
     */
    WorkflowState state();

    /**
     * Verifies that given steps are waiting in the workflow execution.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param stepName                first expected waiting step name
     * @param stepNames               additional optional expected waiting step names
     */
    void waitingIn(AssertionFailureHandler assertionFailureHandler, String stepName, String... stepNames);

    /**
     * Verifies that given steps are waiting in the workflow execution.
     *
     * @param stepName  first expected waiting step name
     * @param stepNames additional optional expected waiting step names
     */
    default void waitingIn(String stepName, String... stepNames) {
        waitingIn(AssertionFailureHandler.handler(false), stepName, stepNames);
    }

    /**
     * Verifies that given steps have passed.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param stepName                first expected passed step name
     * @param stepNames               additional optional expected passed step names
     */
    void stepsPassed(AssertionFailureHandler assertionFailureHandler, String stepName, String... stepNames);

    /**
     * Verifies that given steps have passed.
     *
     * @param stepName  first expected passed step name
     * @param stepNames additional optional expected passed step names
     */
    default void stepsPassed(String stepName, String... stepNames) {
        stepsPassed(AssertionFailureHandler.handler(false), stepName, stepNames);
    }

    /**
     * Verifies that given steps exist in the workflow state.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param stepName                first expected step name
     * @param stepNames               additional optional expected step names
     */
    void hasSteps(AssertionFailureHandler assertionFailureHandler, String stepName, String... stepNames);

    /**
     * Verifies that given steps exist in the workflow state.
     *
     * @param stepName  first expected step name
     * @param stepNames additional optional expected step names
     */
    default void hasSteps(String stepName, String... stepNames) {
        hasSteps(AssertionFailureHandler.handler(false), stepName, stepNames);
    }

    /**
     * Verifies that given steps exist in the workflow state in any order.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param stepName                first expected step name
     * @param stepNames               additional optional expected step names
     */
    void hasStepsInAnyOrder(AssertionFailureHandler assertionFailureHandler, String stepName, String... stepNames);

    /**
     * Verifies that given steps exist in the workflow state in any order.
     *
     * @param stepName  first expected step name
     * @param stepNames additional optional expected step names
     */
    default void hasStepsInAnyOrder(String stepName, String... stepNames) {
        hasStepsInAnyOrder(AssertionFailureHandler.handler(false), stepName, stepNames);
    }

    /**
     * Verifies that given steps exist in the workflow state in the given order.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param stepName                first expected step name
     * @param stepNames               additional optional expected step names
     */
    void hasStepsInOrder(AssertionFailureHandler assertionFailureHandler, String stepName, String... stepNames);

    /**
     * Verifies that given steps exist in the workflow state in the given order.
     *
     * @param stepName  first expected step name
     * @param stepNames additional optional expected step names
     */
    default void hasStepsInOrder(String stepName, String... stepNames) {
        hasStepsInOrder(AssertionFailureHandler.handler(false), stepName, stepNames);
    }

    /**
     * Verifies that given steps do not exist in the workflow state.
     *
     * @param stepName  first expected step name
     * @param stepNames additional optional step names
     */
    void noStep(String stepName, String... stepNames);

    /**
     * Waits until a step satisfies the given predicate.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param predicate               predicate for the workflow step
     * @return the matching workflow step
     */
    WorkflowStep stepMatches(AssertionFailureHandler assertionFailureHandler,
                             Predicate<WorkflowStep> predicate);

    /**
     * Waits until a step satisfies the given predicate.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param predicate               predicate for the workflow step
     * @return the matching workflow step
     */
    default WorkflowStep step(AssertionFailureHandler assertionFailureHandler, Predicate<WorkflowStep> predicate) {
        return stepMatches(assertionFailureHandler, predicate);
    }

    /**
     * Waits until a step satisfies the given predicate.
     *
     * @param predicate predicate for the workflow step
     * @return the matching workflow step
     */
    default WorkflowStep stepMatches(Predicate<WorkflowStep> predicate) {
        return stepMatches(AssertionFailureHandler.handler(false), predicate);
    }

    /**
     * Waits until a step satisfies the given predicate.
     *
     * @param predicate predicate for the workflow step
     * @return the matching workflow step
     */
    default WorkflowStep step(Predicate<WorkflowStep> predicate) {
        return stepMatches(AssertionFailureHandler.handler(false), predicate);
    }

    /**
     * Waits until a workflow step exists.
     *
     * @return the first available workflow step
     */
    default WorkflowStep stepExists() {
        return stepMatches(step -> true);
    }

    /**
     * Applies a consumer to an existing workflow step.
     *
     * @param stepConsumer consumer to apply to the step
     */
    void stepSatisfies(Consumer<WorkflowStep> stepConsumer);

    /**
     * Applies a consumer to the first step matching the predicate.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param predicate               predicate for the workflow step
     * @param stepConsumer            consumer to apply to the matching step
     */
    void stepSatisfies(AssertionFailureHandler assertionFailureHandler,
                       Predicate<WorkflowStep> predicate,
                       Consumer<WorkflowStep> stepConsumer);

    /**
     * Applies a consumer to the first step matching the predicate.
     *
     * @param predicate    predicate for the workflow step
     * @param stepConsumer consumer to apply to the matching step
     */
    default void stepSatisfies(Predicate<WorkflowStep> predicate, Consumer<WorkflowStep> stepConsumer) {
        stepSatisfies(AssertionFailureHandler.handler(false), predicate, stepConsumer);
    }

    /**
     * Waits until the workflow payload matches the given predicate.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param predicate               predicate for the workflow payload
     * @return the matching workflow payload
     */
    Map<String, @Nullable Object> payloadMatches(AssertionFailureHandler assertionFailureHandler,
                                       Predicate<Map<String, @Nullable Object>> predicate);

    /**
     * Waits until the workflow payload matches the given predicate.
     *
     * @param predicate predicate for the workflow payload
     * @return the matching workflow payload
     */
    default Map<String, @Nullable Object> payloadMatches(Predicate<Map<String, @Nullable Object>> predicate) {
        return payloadMatches(AssertionFailureHandler.handler(false), predicate);
    }

    /**
     * Returns the workflow payload after asserting it exists.
     *
     * @return the workflow payload
     */
    Map<String, @Nullable Object> payloadExists();

    /**
     * Applies a consumer to the workflow payload after asserting it exists.
     *
     * @param payloadConsumer consumer for the workflow payload
     */
    void payloadSatisfies(Consumer<Map<String, @Nullable Object>> payloadConsumer);

    /**
     * Waits until the workflow state matches the given predicate.
     *
     * @param assertionFailureHandler handler for assertion failures
     * @param predicate               predicate for the workflow state
     * @return the matching workflow state
     */
    WorkflowState workflowStateMatches(AssertionFailureHandler assertionFailureHandler,
                                       Predicate<WorkflowState> predicate);

    /**
     * Waits until the workflow state matches the given predicate.
     *
     * @param predicate predicate for the workflow state
     * @return the matching workflow state
     */
    default WorkflowState workflowStateMatches(Predicate<WorkflowState> predicate) {
        return workflowStateMatches(AssertionFailureHandler.handler(false), predicate);
    }

    /**
     * Returns the workflow state after asserting it exists.
     *
     * @return the workflow state
     */
    default WorkflowState workflowStateExists() {
        return workflowStateMatches(workflowState -> true);
    }

    /**
     * Applies a consumer to the workflow state.
     *
     * @param workflowStateConsumer consumer for the workflow state
     */
    void workflowStateSatisfies(Consumer<WorkflowState> workflowStateConsumer);
}
