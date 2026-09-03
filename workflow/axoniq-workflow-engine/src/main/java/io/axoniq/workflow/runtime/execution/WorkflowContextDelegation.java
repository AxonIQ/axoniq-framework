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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.PayloadPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.VersionPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowLifecycleControl;
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;

/**
 * Implementation of {@link WorkflowContext} that delegates all operations to the underlying {@link WorkflowExecution}
 * and primitive implementations.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowContextDelegation implements WorkflowContext {

    // Primitive implementations
    private final RetryableExecuteDelegate retryableExecuteDelegate;
    private final WaitForDelegate waitForDelegate;
    private final WorkflowLifecycleControlDelegate lifecycleControlDelegate;
    private final PayloadDelegate payloadDelegate;
    private final VersionDelegate versionDelegate;

    // Combinators
    private final AnyMatchCombinatorDelegate anyCombinatorDelegate;
    private final NoneMatchCombinatorDelegate noneCombinatorDelegate;
    private final AllMatchCombinatorDelegate allCombinatorDelegate;

    // Execution
    private final ProcessingContext processingContext;
    private final WorkflowExecution workflowExecution;
    private final WorkflowContext workflowContext;

    // Services
    private final EventStore eventStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final UnitOfWorkFactory workflowBodyUnitOfWorkFactory;
    private final Clock clock;
    private final ExecutorService executorService;
    private final WorkflowScheduler timeoutScheduler;
    private final ExecuteStepActionResolver executeStepActionResolver;


    /**
     * Creates the context delegation.
     *
     * @param workflowConfiguration workflow configuration
     * @param workflowContext       workflow context created by the factory
     * @param workflowExecution     workflow execution
     * @param runningSteps          running step registry
     * @param eventWaitConditions   event wait condition registry
     * @param reachedSteps          reached steps tracker
     * @param terminalTransition    owner of workflow terminal-transition execution mechanics
     * @param processingContext     processing context
     */
    public WorkflowContextDelegation(
            WorkflowConfiguration<?> workflowConfiguration,
            WorkflowContext workflowContext,
            WorkflowExecution workflowExecution,
            RunningSteps runningSteps,
            EventWaitConditions eventWaitConditions,
            ReachedSteps reachedSteps,
            WorkflowTerminalTransition terminalTransition,
            ProcessingContext processingContext
    ) {

        var stepParent = workflowConfiguration.eventNameCustomizer().forStepInheritance();
        this.workflowContext = workflowContext;

        this.workflowExecution = Objects.requireNonNull(
                workflowExecution,
                "Workflow execution supplier must not be null");
        this.processingContext = Objects.requireNonNull(
                processingContext,
                "Processing context must not be null");
        this.unitOfWorkFactory = Objects.requireNonNull(
                processingContext.component(UnitOfWorkFactory.class),
                "Could not retrieve UoW factory");
        this.workflowBodyUnitOfWorkFactory = new SimpleUnitOfWorkFactory(processingContext);
        this.clock = Objects.requireNonNull(
                processingContext.component(Clock.class),
                "Could not retrieve Clock");
        this.executorService = Objects.requireNonNull(
                processingContext.component(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR),
                "Could not retrieve workflow engine executor");
        this.eventStore = Objects.requireNonNull(
                processingContext.component(EventStore.class),
                "Could not retrieve EventStore");
        this.timeoutScheduler = Objects.requireNonNull(
                processingContext.component(WorkflowScheduler.class),
                "Could not retrieve WorkflowScheduler");
        this.executeStepActionResolver = Objects.requireNonNull(
                processingContext.component(ExecuteStepActionResolver.class),
                "Could not retrieve ExecuteStepActionResolver");

        var executeDelegate = new ExecuteDelegate(workflowContext,
                                                  workflowExecution,
                                                  runningSteps,
                                                  reachedSteps,
                                                  stepParent,
                                                  clock,
                                                  unitOfWorkFactory,
                                                  executorService,
                                                  timeoutScheduler,
                                                  executeStepActionResolver);
        this.retryableExecuteDelegate = new RetryableExecuteDelegate(executeDelegate,
                                                                     workflowContext,
                                                                     workflowExecution,
                                                                     runningSteps,
                                                                     reachedSteps,
                                                                     stepParent,
                                                                     clock,
                                                                     timeoutScheduler);
        this.waitForDelegate = new WaitForDelegate(workflowContext,
                                                   workflowExecution,
                                                   runningSteps,
                                                   eventWaitConditions,
                                                   reachedSteps,
                                                   stepParent,
                                                   clock,
                                                   timeoutScheduler);
        this.lifecycleControlDelegate = new WorkflowLifecycleControlDelegate(workflowContext,
                                                                             workflowExecution,
                                                                             runningSteps,
                                                                             reachedSteps,
                                                                             terminalTransition);
        this.payloadDelegate = new PayloadDelegate(workflowContext,
                                                   workflowExecution,
                                                   runningSteps,
                                                   reachedSteps,
                                                   stepParent,
                                                   clock,
                                                   timeoutScheduler);
        this.versionDelegate = new VersionDelegate(workflowContext,
                                                   workflowExecution,
                                                   reachedSteps,
                                                   stepParent,
                                                   clock);

        this.anyCombinatorDelegate = new AnyMatchCombinatorDelegate(workflowExecution);
        this.noneCombinatorDelegate = new NoneMatchCombinatorDelegate(workflowExecution);
        this.allCombinatorDelegate = new AllMatchCombinatorDelegate(workflowExecution);
    }

    /**
     * Returns the identifier of the workflow instance.
     *
     * @return the workflow instance identifier
     */
    @Override
    public String workflowId() {
        return workflowExecution.workflowId();
    }

    /**
     * Returns the version of the workflow definition being executed.
     *
     * @return the workflow definition version
     */
    @Override
    public String workflowVersion() {
        return workflowExecution.state().workflowDefinitionId().version();
    }

    /**
     * Returns the current workflow payload.
     *
     * @return the workflow payload
     */
    @Override
    public Map<String, @Nullable Object> workflowPayload() {
        return workflowExecution.state().payload();
    }

    /**
     * Changes the workflow payload according to the given command.
     *
     * @param command the payload modification to perform
     * @return the result representing the modification
     */
    @Override
    public WorkflowStepResult modifyPayload(PayloadPrimitive.ModifyPayloadCommand command) {
        workflowExecution.state().throwTerminalCause();
        return payloadDelegate.modifyPayload(command);
    }

    /**
     * Returns the current status of the workflow.
     *
     * @return the workflow status
     */
    @Override
    public WorkflowStatus workflowStatus() {
        return workflowExecution.state().workflowStatus();
    }

    /**
     * Returns the names of all known workflow steps.
     *
     * @return the workflow step names
     */
    @Override
    public List<String> workflowStepNames() {
        return workflowExecution.state().workflowStepNames();
    }

    /**
     * Returns the processing context of this workflow invocation.
     *
     * @return the processing context
     */
    @Override
    public ProcessingContext processingContext() {
        return this.processingContext;
    }

    // delegation
    /**
     * Executes the step described by the given command.
     *
     * @param command the step execution command
     * @return the result representing the step execution
     */
    @Override
    public WorkflowStepResult execute(ExecutePrimitive.ExecuteCommand command) {
        workflowExecution.state().throwTerminalCause();
        return retryableExecuteDelegate.execute(command);
    }

    /**
     * Waits for an event according to the given command.
     *
     * @param command the event wait command
     * @return the result representing the event wait
     */
    @Override
    public WorkflowStepResult waitForEvent(WaitForPrimitive.WaitForCommand command) {
        workflowExecution.state().throwTerminalCause();
        return waitForDelegate.waitForEvent(command);
    }

    /**
     * Resolves the workflow version according to the given command.
     *
     * @param command the version resolution command
     * @return the result representing the version resolution
     */
    @Override
    public WorkflowStepResult version(VersionPrimitive.VersionCommand command) {
        workflowExecution.state().throwTerminalCause();
        return versionDelegate.version(command);
    }

    /**
     * Requests cancellation of the workflow.
     *
     * @param command the workflow cancellation command
     */
    @Override
    public void cancelWorkflow(WorkflowLifecycleControl.CancelWorkflowCommand command) {
        workflowExecution.state().throwTerminalCause();
        lifecycleControlDelegate.cancelWorkflow(PrimitiveCommands.cancelWorkflow(
                command.cause(),
                merge(workflowExecution.workflowConfiguration().eventNameCustomizer(), command.eventNameCustomizer())
        ));
    }

    /**
     * Fails the workflow with the cause supplied by the command.
     *
     * @param command the workflow failure command
     */
    @Override
    public void failWorkflow(WorkflowLifecycleControl.FailWorkflowCommand command) {
        workflowExecution.state().throwTerminalCause();
        lifecycleControlDelegate.failWorkflow(PrimitiveCommands.failWorkflow(
                command.cause(),
                merge(workflowExecution.workflowConfiguration().eventNameCustomizer(), command.eventNameCustomizer())
        ));
    }

    /**
     * Requests cancellation of a workflow step.
     *
     * @param command the step cancellation command
     * @return {@code true} when the cancellation was accepted
     */
    @Override
    public boolean cancelStep(WorkflowLifecycleControl.CancelStepCommand command) {
        return lifecycleControlDelegate.cancelStep(command);
    }

    /**
     * Combines step results when all results satisfy the predicate.
     *
     * @param predicate predicate each result must satisfy
     * @param results results to combine
     * @return the combined step result
     */
    @Override
    public CombinatorWorkflowStepResult allMatch(Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return allCombinatorDelegate.allMatch(predicate, results);
    }

    /**
     * Combines step results when any result satisfies the predicate.
     *
     * @param predicate predicate at least one result must satisfy
     * @param results results to combine
     * @return the combined step result
     */
    @Override
    public CombinatorWorkflowStepResult anyMatch(Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return anyCombinatorDelegate.anyMatch(predicate, results);
    }

    /**
     * Combines step results when no result satisfies the predicate.
     *
     * @param predicate predicate no result may satisfy
     * @param results results to combine
     * @return the combined step result
     */
    @Override
    public CombinatorWorkflowStepResult noneMatch(Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return noneCombinatorDelegate.noneMatch(predicate, results);
    }


    EventStore eventStore() {
        return eventStore;
    }

    /**
     * Returns the clock used by workflow primitives.
     *
     * @return the workflow clock
     */
    public Clock clock() {
        return this.clock;
    }

    /**
     * Returns the transactional unit of work factory for short-lived workflow operations.
     *
     * @return the transactional unit of work factory
     */
    public UnitOfWorkFactory unitOfWorkFactory() {
        return this.unitOfWorkFactory;
    }

    /**
     * Factory for the unit of work wrapping a workflow instance's body execution.
     * <p>
     * The body-wrapper unit of work stays open for the workflow instance's entire lifetime, including any time the body
     * spends parked at {@code awaitEvent}, {@code sleep}, or retry backoff. It is therefore non-transactional, as a
     * transaction bound to it would hold its resources per in-flight instance for as long as the instance lives.
     * Workflow and step events are published in short-lived child units of work created from the transactional
     * {@link #unitOfWorkFactory()} instead.
     *
     * @return the non-transactional unit of work factory used for the workflow-body wrapper
     */
    public UnitOfWorkFactory workflowBodyUnitOfWorkFactory() {
        return this.workflowBodyUnitOfWorkFactory;
    }

    /**
     * Returns the executor that runs workflow work.
     *
     * @return the workflow executor
     */
    public ExecutorService executorService() {
        return this.executorService;
    }

    /**
     * Dispatches an event to the wait-for primitive.
     *
     * @param awaited event arrival wrapper object
     */
    @Internal
    public void eventReceived(EventWaitConditions.Awaited awaited) {
        waitForDelegate.eventReceived(awaited);
    }

    /**
     * Retrieves typed version of the workflow context.
     *
     * @param <T> workflow context type
     * @return typed workflow context created by the factory
     */
    public <T extends WorkflowContext> T typedWorkflowContext() {
        try {
            //noinspection unchecked
            return (T) this.workflowContext;
        } catch (ClassCastException e) {
            throw new WorkflowFailedException("Failed to cast workflow context", e);
        }
    }

    /**
     * Describes this workflow context to the supplied component descriptor.
     *
     * @param descriptor descriptor to populate
     */
    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("workflowId", workflowExecution.workflowId());
        descriptor.describeProperty("workflowName", workflowExecution.workflowName());
    }
}
