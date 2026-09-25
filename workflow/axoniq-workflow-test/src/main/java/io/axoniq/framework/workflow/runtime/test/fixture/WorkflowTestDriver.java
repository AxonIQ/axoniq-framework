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

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.PayloadProcessor;
import io.axoniq.framework.workflow.runtime.test.configuration.WorkflowTestSteppingModeEnhancer;
import org.awaitility.core.ThrowingRunnable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Imperative test driver for workflow runtime tests.
 *
 * <p>This type exposes the low-level primitives that power {@link WorkflowTestFixture}. Use it directly when a test
 * needs more control than the staged fixture API provides, for example when selecting a specific workflow execution,
 * advancing time explicitly, or driving individual workflow steps in a precise order.</p>
 *
 * <p>The term <em>driver</em> is used here in the modern testing sense: this class is a small facade that drives the
 * system under test and keeps test code thin by wrapping workflow-specific interactions behind a focused API. It is
 * not a classical integration-testing driver in the driver/stub sense where a temporary caller is built because the
 * real upstream component does not exist yet.</p>
 *
 * <p>Create an instance in one of two modes:</p>
 * <ul>
 *     <li>{@link #stepper(WorkflowModule, boolean, UnaryOperator)} starts the workflow engine in stepping mode. In this
 *     mode workflow-defined step actions are paused until the test explicitly executes them through
 *     {@link #executeStep(String)}, {@link #executeStep(String, PayloadProcessor)}, or
 *     {@link #executeStepFailing(String, StepFailedException)}. Stepping mode also enables virtual time control through
 *     {@link #timePasses(Duration)}.</li>
 *     <li>{@link #live(WorkflowModule, UnaryOperator)} or
 *     {@link #live(WorkflowModule, boolean, UnaryOperator)} starts the workflow engine in live mode. In this mode the
 *     workflow runs normally, and the driver is limited to observation and event publication. Methods that depend on
 *     manual step execution or virtual time control reject live mode with an {@link IllegalStateException}.</li>
 * </ul>
 *
 * <p>The driver is stateful. Selection methods such as {@link #executionExists()}, {@link #executionMatches(Predicate)},
 * {@link #historyExists()}, and {@link #historyMatches(Predicate)} update the shared {@link WorkflowEngineTestingState}
 * with the current execution and workflow history. Later operations that inspect the workflow state, especially step
 * execution assertions, operate against that selected state.</p>
 *
 * <p>Most observation methods are eventually consistent and use {@link org.awaitility.Awaitility} under the hood. They
 * wait until the expected execution or history entry appears and then surface the last assertion failure when the wait
 * times out. This makes the driver suitable for workflows that advance asynchronously in response to published events
 * or scheduled timeouts.</p>
 *
 * <p>A typical stepping-mode test flow is:</p>
 * <ol>
 *     <li>Create the driver with one of the {@code stepper(...)} factory methods</li>
 *     <li>Trigger the workflow with {@link #publishEvent(Object)}</li>
 *     <li>Select the active execution with {@link #executionExists()} or {@link #executionMatches(Predicate)}</li>
 *     <li>Drive waiting steps with {@link #executeStep(String)}, {@link #executeStep(String, PayloadProcessor)}, or
 *     {@link #executeStepFailing(String, StepFailedException)}</li>
 *     <li>Advance scheduled work with {@link #timePasses(Duration)} when the workflow waits on time</li>
 *     <li>Assert the resulting execution or recorded history with {@link #executionSatisfies(Consumer)},
 *     {@link #historySatisfies(Consumer)}, {@link #noExecution()}, or {@link #noHistory()}</li>
 * </ol>
 *
 * <p>Tests that create the driver directly should call {@link #shutdown()} when finished so all workflow infrastructure
 * is stopped cleanly.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowTestDriver {

    /**
     * Constructs a new test driver in stepping mode.
     *
     * @param workflowModule workflow module
     * @param throwFixtureException whether to fail assertions. If set to {@code true}, the failures are delivered as
     * {@link WorkflowTestFixtureException}s, else as {@link AssertionError}
     * @param customize customizer of the workflow configuration
     * @param <C> type of the workflow context
     * @return workflow test driver
     */
    static <C extends WorkflowContext> WorkflowTestDriver stepper(
            WorkflowModule<C> workflowModule,
            boolean throwFixtureException,
            UnaryOperator<WorkflowConfigurer> customize) {

        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(r -> new WorkflowTestSteppingModeEnhancer().enhance(r))
                  .registerWorkflowModule(
                          Objects.requireNonNull(workflowModule, "Workflow module must not be null"));

        var configuration = Objects.requireNonNull(customize, "Customizer must not be null")
                                   .apply(configurer).start();

        var workflowTestServices = WorkflowTestServices.from(configuration);
        var assertionFailureHandler = AssertionFailureHandler.handler(throwFixtureException);
        return new DefaultWorkflowTestDriver(workflowTestServices,
                                             new MutableWorkflowEngineTestingState(),
                                             assertionFailureHandler,
                                             true);
    }

    /**
     * Creates a new test driver in live mode.
     *
     * @param workflowModule workflow module
     * @param customize customizer of the workflow configuration
     * @param <C> type of the workflow context
     * @return workflow test driver
     */
    static <C extends WorkflowContext> WorkflowTestDriver live(
            WorkflowModule<C> workflowModule,
            UnaryOperator<WorkflowConfigurer> customize) {
        return live(
                Objects.requireNonNull(workflowModule, "Workflow module must not be null"),
                false,
                Objects.requireNonNull(customize, "Customizer must not be null"));
    }

    /**
     * Creates a new test driver in live mode.
     *
     * @param workflowModule workflow module
     * @param throwFixtureException whether to fail assertions with {@link WorkflowTestFixtureException}s
     * @param customize customizer of the workflow configuration
     * @param <C> type of the workflow context
     * @return workflow test driver
     */
    static <C extends WorkflowContext> WorkflowTestDriver live(
            WorkflowModule<C> workflowModule,
            boolean throwFixtureException,
            UnaryOperator<WorkflowConfigurer> customize) {
        var configurer = WorkflowConfigurer.create();
        configurer.registerWorkflowModule(Objects.requireNonNull(workflowModule, "Workflow module must not be null"));
        var configuration = Objects.requireNonNull(customize, "Customizer must not be null").apply(configurer).start();
        var assertionFailureHandler = AssertionFailureHandler.handler(throwFixtureException);
        var workflowTestServices = WorkflowTestServices.from(configuration);
        return new DefaultWorkflowTestDriver(workflowTestServices,
                                             new MutableWorkflowEngineTestingState(),
                                             assertionFailureHandler,
                                             false);
    }

    /**
     * Constructs a list of step names.
     *
     * @param stepName mandatory first step name
     * @param stepNames optional additional step names
     * @return list of steps
     */
    static List<String> stepNames(String stepName, String... stepNames) {
        var allNames = new ArrayList<String>();
        allNames.add(Objects.requireNonNull(stepName, "Step name must not be null"));
        allNames.addAll(Arrays.asList(stepNames));
        return allNames;
    }

    /**
     * Returns services for testing workflow runtime.
     *
     * @return services for testing workflow runtime
     */
    WorkflowTestServices workflowTestServices();

    /**
     * Returns the shared testing state.
     *
     * @return shared testing state
     */
    WorkflowEngineTestingState testingState();

    /**
     * Returns the assertion failure handler.
     *
     * @return assertion failure handler
     */
    AssertionFailureHandler assertionFailureHandler();

    /**
     * Indicates whether this driver runs in stepping mode.
     *
     * @return {@code true} if this driver runs in stepping mode, otherwise {@code false}
     */
    boolean steppingMode();

    /**
     * Waits until an active workflow execution exists.
     *
     * @return the selected execution
     */
    WorkflowExecution executionExists();

    /**
     * Waits until an active workflow execution matching the predicate exists.
     *
     * @param predicate predicate used to select matching executions
     * @return the selected execution
     */
    WorkflowExecution executionMatches(Predicate<WorkflowExecution> predicate);

    /**
     * Waits until an active workflow execution exists and applies the given consumer.
     *
     * @param executionConsumer consumer applied to the selected execution
     */
    void executionSatisfies(Consumer<WorkflowExecution> executionConsumer);

    /**
     * Waits until no active workflow executions exist.
     */
    void noExecution();

    /**
     * Waits until a workflow history matching the predicate exists.
     *
     * @param predicate predicate used to select matching histories
     * @return the selected history
     */
    WorkflowHistory historyMatches(Predicate<WorkflowHistory> predicate);

    /**
     * Waits until a workflow history exists.
     *
     * @return the selected history
     */
    WorkflowHistory historyExists();

    /**
     * Waits until a workflow history exists and applies the given consumer.
     *
     * @param historyConsumer consumer applied to the selected history
     */
    void historySatisfies(Consumer<WorkflowHistory> historyConsumer);

    /**
     * Asserts that no workflow histories exist.
     */
    void noHistory();

    /**
     * Publishes an event.
     *
     * @param event event message or event payload
     */
    void publishEvent(Object event);

    /**
     * Advances time by the given duration.
     *
     * @param duration duration to advance time by
     */
    void timePasses(Duration duration);

    /**
     * Waits until the given step is executed calling original action defined in the workflow definition.
     *
     * <p>This works only in stepping mode.</p>
     *
     * @param stepName step name
     */
    void executeStep(String stepName);

    /**
     * Waits until the given step is executed calling the given payload processor.
     *
     * <p>This works only in stepping mode.</p>
     *
     * @param stepName step name
     * @param payloadProcessor payload processor to use instead of the workflow-defined action
     */
    void executeStep(String stepName, PayloadProcessor payloadProcessor);

    /**
     * Waits until the given step is executed throwing the given exception.
     *
     * <p>This works only in stepping mode.</p>
     *
     * @param stepName name of the step to execute
     * @param exception exception to throw
     */
    void executeStepFailing(String stepName, StepFailedException exception);

    /**
     * Shuts down the tester.
     */
    void shutdown();

    /**
     * Waits until the given condition is met, using the defined assertion failure handler.
     *
     * @param description description of the assertion
     * @param assertion assertion to execute
     */
    void awaitEventually(String description, ThrowingRunnable assertion);
}
