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
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Base class for fixture phases that share workflow selection and lifecycle operations.
 *
 * <p>{@link GivenWhen} and {@link Then} both build on this class. It binds a phase instance to the
 * {@link WorkflowTestFixture} that created it and to the shared {@link WorkflowTestDriver} that performs the actual
 * workflow interaction. This keeps the common phase behavior in one place while allowing the concrete phase types to
 * add action-specific or assertion-specific methods.</p>
 *
 * <p>The methods in this class are available in both action and assertion phases because they operate on shared fixture
 * state: selecting the current execution, selecting workflow history, asserting absence of executions or history, and
 * shutting the fixture down. Selection methods update the driver's {@link WorkflowEngineTestingState}, so later calls in
 * the same fluent chain can assert against the selected execution or history without having to pass identifiers
 * repeatedly.</p>
 *
 * <p>The self-referential generic parameter preserves the concrete phase type for fluent chaining. Subclasses therefore
 * inherit these shared operations without losing their own typed API surface.</p>
 *
 * @param <SELF> concrete phase type used for fluent chaining
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
abstract class FixturePhase<SELF extends FixturePhase<SELF>> extends Phase<SELF> {

    protected WorkflowTestFixture<?, ?> fixture;
    protected WorkflowTestDriver testDriver;

    /**
     * Initializes this phase with the owning fixture and the shared test driver.
     *
     * <p>The fixture calls this during construction so both phases operate on the same underlying workflow runtime and
     * testing state.</p>
     *
     * @param fixture owning workflow test fixture
     * @param testDriver shared workflow test driver
     */
    protected void initialize(@Nonnull WorkflowTestFixture<?, ?> fixture,
                              @Nonnull WorkflowTestDriver testDriver) {
        Objects.requireNonNull(fixture, "Fixture must not be null");
        Objects.requireNonNull(testDriver, "Test driver must not be null");
        this.fixture = fixture;
        this.testDriver = testDriver;
    }

    /**
     * Waits until any active workflow execution exists.
     *
     * @return this phase for fluent chaining
     */
    public SELF executionExists() {
        testDriver.executionExists();
        return self();
    }

    /**
     * Waits until an active workflow execution matching the predicate exists.
     * <p>
     * If exactly one execution matches, it becomes the selected execution for subsequent assertions.
     *
     * @param predicate predicate used to select matching executions
     * @return this phase for fluent chaining
     */
    public SELF executionExists(@Nonnull Predicate<WorkflowExecution> predicate) {
        Objects.requireNonNull(predicate, "Execution predicate must not be null");
        testDriver.executionMatches(predicate);
        return self();
    }

    /**
     * Waits until a workflow history matching the predicate exists.
     * <p>
     * If exactly one history matches, it becomes the selected history for subsequent assertions.
     *
     * @param predicate predicate used to select matching histories
     * @return this phase for fluent chaining
     */
    public SELF historyExists(@Nonnull Predicate<WorkflowHistory> predicate) {
        Objects.requireNonNull(predicate, "History predicate must not be null");
        testDriver.historyMatches(predicate);
        return self();
    }

    /**
     * Waits until a workflow history exists for the given workflow id.
     *
     * @param workflowId workflow id to find
     * @return this phase for fluent chaining
     */
    public SELF historyExists(@Nonnull String workflowId) {
        Objects.requireNonNull(workflowId, "Workflow id must not be null");
        return historyExists(h -> workflowId.equals(h.workflowId()));
    }

    /**
     * Waits until at least one workflow history exists and has the given status.
     *
     * @param workflowStatus expected workflow status
     * @return this phase for fluent chaining
     */
    public SELF historyExists(@Nonnull WorkflowStatus workflowStatus) {
        Objects.requireNonNull(workflowStatus, "Workflow status must not be null");
        testDriver.historyMatches(history -> history.state().workflowStatus().equals(workflowStatus));
        return self();
    }

    /**
     * Asserts that no active workflow execution is present.
     *
     * @return this phase for fluent chaining
     */
    public SELF noExecution() {
        testDriver.noExecution();
        return self();
    }

    /**
     * Asserts that no workflow history is recorded.
     *
     * @return this phase for fluent chaining
     */
    public SELF noHistory() {
        testDriver.noHistory();
        return self();
    }

    /**
     * Shuts down the test driver and all services.
     */
    public void stop() {
        testDriver.shutdown();
    }
}
