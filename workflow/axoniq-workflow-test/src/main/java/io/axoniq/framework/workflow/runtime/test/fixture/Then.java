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

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.common.FutureUtils;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A stage for defining the workflow assertions.
 *
 * @param <SELF>   type of assert stage
 * @param <ACTION> type of action stage
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class Then<SELF extends Then<SELF, ACTION>, ACTION extends GivenWhen<ACTION, SELF>>
        extends FixturePhase<SELF> {

    /**
     * Default implementation of the {@link Then}.
     */
    public static final class Phase extends Then<Then.Phase, GivenWhen.Phase> {

    }

    /**
     * Returns the typed fixture to use.
     *
     * @return fixture to continue fluent invocations
     */
    @SuppressWarnings("unchecked")
    public WorkflowTestFixture<ACTION, SELF> and() {
        return (WorkflowTestFixture<ACTION, SELF>) fixture;
    }

    /**
     * Asserts that the selected workflow execution is waiting in every given step.
     * <p>
     * A waiting step is present and not in a terminal state.
     *
     * @param stepName  first expected waiting step name
     * @param stepNames additional expected waiting step names
     * @return this phase for fluent chaining
     */
    public SELF waitingIn(String stepName, String... stepNames) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        testDriver.testingState().waitingIn(this.testDriver.assertionFailureHandler(), stepName, stepNames);
        return self();
    }

    /**
     * Asserts that every given step has been passed.
     * <p>
     * A passed step is present in the selected execution or history and is in a terminal state.
     *
     * @param stepName  first expected passed step name
     * @param stepNames additional expected passed step names
     * @return this phase for fluent chaining
     */
    public SELF stepsPassed(String stepName, String... stepNames) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        testDriver.testingState().stepsPassed(this.testDriver.assertionFailureHandler(), stepName, stepNames);
        return self();
    }

    /**
     * Asserts that every given step exists in the selected workflow state.
     * <p>
     * The selected workflow state can be an active execution or a stored history.
     *
     * @param stepName  first expected step name
     * @param stepNames additional expected step names
     * @return this phase for fluent chaining
     */
    public SELF hasSteps(String stepName, String... stepNames) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        testDriver.testingState().hasSteps(this.testDriver.assertionFailureHandler(), stepName, stepNames);
        return self();
    }

    /**
     * Asserts that the selected workflow state contains exactly the given steps in any order.
     *
     * @param stepName  first expected step name
     * @param stepNames additional expected step names
     * @return this phase for fluent chaining
     */
    public SELF hasStepsInAnyOrder(String stepName, String... stepNames) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        testDriver.testingState().hasStepsInAnyOrder(this.testDriver.assertionFailureHandler(), stepName, stepNames);
        return self();
    }

    /**
     * Asserts that the selected workflow state does not contain the given step.
     *
     * @param stepName step name that must be absent
     * @return this phase for fluent chaining
     */
    public SELF noStep(String stepName) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        testDriver.testingState().noStep(stepName);
        return self();
    }

    /**
     * Asserts that the selected workflow state is not terminal.
     *
     * @return this phase for fluent chaining
     */
    public SELF workflowNotFinished() {
        testDriver.awaitEventually("Expected current workflow to remain non-terminal", () -> {
            var workflowStatus = testDriver.testingState().state().workflowStatus();
            if (workflowStatus.isTerminal()) {
                throw new AssertionError(
                        "Expected current workflow to remain non-terminal, but workflow status is %s."
                                .formatted(workflowStatus)
                );
            }
        });
        return self();
    }

    /**
     * Asserts that the workflow has finished with the given status.
     *
     * @param workflowStatus expected terminal workflow status
     * @return this phase for fluent chaining
     */
    public SELF workflowFinished(WorkflowStatus workflowStatus) {
        Objects.requireNonNull(workflowStatus, "Workflow status must not be null");
        noExecution();
        testDriver.awaitEventually("Expected workflow to finish with status %s".formatted(workflowStatus), () -> {
            var matchingHistories = FutureUtils.joinAndUnwrap(testDriver.workflowTestServices()
                                                                         .workflowHistoryRepository()
                                                                         .findAll())
                                             .stream()
                                             .filter(history -> history.state().workflowStatus() == workflowStatus)
                                             .toList();
            if (matchingHistories.isEmpty()) {
            var actualStatuses = FutureUtils.joinAndUnwrap(testDriver.workflowTestServices()
                                                                       .workflowHistoryRepository()
                                                                       .findAll())
                                               .stream()
                                               .map(history -> history.state().workflowStatus())
                                               .toList();
                throw new AssertionError(
                        "Expected workflow to finish with status %s, but recorded workflow history statuses are %s."
                                .formatted(workflowStatus, actualStatuses)
                );
            }
            ((DefaultWorkflowTestDriver) testDriver).mutableTestingState().setHistory(matchingHistories.getFirst());
        });
        return self();
    }

    /**
     * Applies a given consumer to the selected workflow history payload.
     *
     * @param payloadConsumer consumer for payload retrieved from the current testing state.
     * @return this phase for fluent chaining
     */
    public SELF payloadSatisfies(Consumer<Map<String, @Nullable Object>> payloadConsumer) {
        Objects.requireNonNull(payloadConsumer, "Payload consumer must not be null");
        payloadConsumer.accept(testDriver.testingState().state().payload());
        return self();
    }

    /**
     * Asserts that the selected workflow history payload satisfies the given predicate.
     *
     * @param predicate predicate to test the payload against
     * @return this phase for fluent chaining
     */
    public SELF payloadMatches(Predicate<Map<String, @Nullable Object>> predicate) {
        Objects.requireNonNull(predicate, "Payload predicate must not be null");
        testDriver.testingState().payloadMatches(this.testDriver.assertionFailureHandler(), predicate);
        return self();
    }

    /**
     * Asserts that the selected workflow history payload equals the expected payload.
     *
     * @param expectedPayload expected workflow payload
     * @return this phase for fluent chaining
     */
    public SELF payloadEquals(Map<String, @Nullable Object> expectedPayload) {
        Objects.requireNonNull(expectedPayload, "Payload must not be null");
        return payloadMatches(p -> p.equals(expectedPayload));
    }

    /**
     * Asserts that the selected workflow history payload contains the expected payload.
     *
     * @param expectedPayload expected workflow payload
     * @return this phase for fluent chaining
     */
    public SELF payloadContains(Map<String, @Nullable Object> expectedPayload) {
        Objects.requireNonNull(expectedPayload, "Payload must not be null");
        return payloadMatches(p -> p.entrySet().containsAll(expectedPayload.entrySet()));
    }

    /**
     * Applies a given consumer to the workflow state of the current history.
     *
     * @param workflowStateConsumer workflow state consumer
     * @return this phase for fluent chaining
     */
    public SELF workflowStateSatisfies(Consumer<WorkflowState> workflowStateConsumer) {
        Objects.requireNonNull(workflowStateConsumer, "Consumer must not be null");
        workflowStateConsumer.accept(testDriver.testingState().state());
        return self();
    }

    /**
     * Asserts that a step has the expected execution status.
     *
     * @param stepName   step name to inspect
     * @param stepStatus expected step status
     * @return this phase for fluent chaining
     */
    public SELF step(String stepName, StepStatus stepStatus) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        Objects.requireNonNull(stepStatus, "Step status must not be null");
        testDriver.testingState().step(this.testDriver.assertionFailureHandler(),
                s -> s.stepName().equals(stepName) && s.status() == stepStatus
        );
        return self();
    }
}
