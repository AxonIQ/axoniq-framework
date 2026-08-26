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
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;

/**
 * Implementation of {@link WorkflowContext} that delegates all operations to the underlying {@link WorkflowExecution}
 * and primitive implementations.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
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
    private final EventSink eventSink;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final UnitOfWorkFactory workflowBodyUnitOfWorkFactory;
    private final Clock clock;
    private final ExecutorService executorService;
    private final WorkflowScheduler timeoutScheduler;
    private final ExecuteStepActionResolver executeStepActionResolver;


    /**
     * Creates the context delegation.
     *
     * @param workflowConfiguration   workflow configuration
     * @param workflowContext        workflow context created by the factory
     * @param workflowExecution      workflow execution
     * @param runningSteps          running step registry
     * @param eventWaitConditions   event wait condition registry
     * @param reachedSteps          reached steps tracker
     * @param terminalTransition    owner of workflow terminal-transition execution mechanics
     * @param processingContext     processing context
     */
    public WorkflowContextDelegation(
            @Nonnull WorkflowConfiguration<?> workflowConfiguration,
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull RunningSteps runningSteps,
            @Nonnull EventWaitConditions eventWaitConditions,
            @Nonnull ReachedSteps reachedSteps,
            @Nonnull WorkflowTerminalTransition terminalTransition,
            @Nonnull ProcessingContext processingContext
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
        this.eventSink = Objects.requireNonNull(
                processingContext.component(EventSink.class),
                "Could not retrieve EventSink");
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
                                                  eventSink,
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
                                                                     unitOfWorkFactory,
                                                                     eventSink,
                                                                     executorService,
                                                                     timeoutScheduler);
        this.waitForDelegate = new WaitForDelegate(workflowContext,
                                                   workflowExecution,
                                                   runningSteps,
                                                   eventWaitConditions,
                                                   reachedSteps,
                                                   stepParent,
                                                   clock,
                                                   unitOfWorkFactory,
                                                   eventSink,
                                                   executorService,
                                                   timeoutScheduler);
        this.lifecycleControlDelegate = new WorkflowLifecycleControlDelegate(workflowContext,
                                                                             workflowExecution,
                                                                             runningSteps,
                                                                             reachedSteps,
                                                                             terminalTransition,
                                                                             unitOfWorkFactory,
                                                                             eventSink,
                                                                             executorService);
        this.payloadDelegate = new PayloadDelegate(workflowContext,
                                                   workflowExecution,
                                                   runningSteps,
                                                   reachedSteps,
                                                   stepParent,
                                                   clock,
                                                   unitOfWorkFactory,
                                                   eventSink,
                                                   executorService,
                                                   timeoutScheduler);
        this.versionDelegate = new VersionDelegate(workflowContext,
                                                   workflowExecution,
                                                   reachedSteps,
                                                   stepParent,
                                                   clock,
                                                   unitOfWorkFactory,
                                                   eventSink,
                                                   executorService);

        this.anyCombinatorDelegate = new AnyMatchCombinatorDelegate(workflowExecution);
        this.noneCombinatorDelegate = new NoneMatchCombinatorDelegate(workflowExecution);
        this.allCombinatorDelegate = new AllMatchCombinatorDelegate(workflowExecution);
    }

    @Nonnull
    @Override
    public String workflowId() {
        return workflowExecution.workflowId();
    }

    @Nonnull
    @Override
    public String workflowVersion() {
        return workflowExecution.state().workflowDefinitionVersion();
    }

    @Nonnull
    @Override
    public Map<String, Object> workflowPayload() {
        return workflowExecution.state().payload();
    }

    @Override
    @Nonnull
    public WorkflowStepResult modifyPayload(@Nonnull PayloadPrimitive.ModifyPayloadCommand command) {
        workflowExecution.state().throwTerminalCause();
        return payloadDelegate.modifyPayload(command);
    }

    @Nonnull
    @Override
    public WorkflowStatus workflowStatus() {
        return workflowExecution.state().workflowStatus();
    }

    @Nonnull
    @Override
    public List<String> workflowStepNames() {
        return workflowExecution.state().workflowStepNames();
    }

    @Nonnull
    @Override
    public ProcessingContext processingContext() {
        return this.processingContext;
    }

    // delegation
    @Override
    @Nonnull
    public WorkflowStepResult execute(@Nonnull ExecutePrimitive.ExecuteCommand command) {
        workflowExecution.state().throwTerminalCause();
        return retryableExecuteDelegate.execute(command);
    }

    @Override
    @Nonnull
    public WorkflowStepResult waitForEvent(@Nonnull WaitForPrimitive.WaitForCommand command) {
        workflowExecution.state().throwTerminalCause();
        return waitForDelegate.waitForEvent(command);
    }

    @Override
    @Nonnull
    public WorkflowStepResult version(@Nonnull VersionPrimitive.VersionCommand command) {
        workflowExecution.state().throwTerminalCause();
        return versionDelegate.version(command);
    }

    @Override
    public void cancelWorkflow(@Nonnull WorkflowLifecycleControl.CancelWorkflowCommand command) {
        workflowExecution.state().throwTerminalCause();
        lifecycleControlDelegate.cancelWorkflow(PrimitiveCommands.cancelWorkflow(
                command.cause(),
                merge(workflowExecution.workflowConfiguration().eventNameCustomizer(), command.eventNameCustomizer())
        ));
    }

    @Override
    public void failWorkflow(@Nonnull WorkflowLifecycleControl.FailWorkflowCommand command) {
        workflowExecution.state().throwTerminalCause();
        lifecycleControlDelegate.failWorkflow(PrimitiveCommands.failWorkflow(
                command.cause(),
                merge(workflowExecution.workflowConfiguration().eventNameCustomizer(), command.eventNameCustomizer())
        ));
    }

    @Override
    public boolean cancelStep(@Nonnull WorkflowLifecycleControl.CancelStepCommand command) {
        return lifecycleControlDelegate.cancelStep(command);
    }

    @Nonnull
    @Override
    public CombinatorWorkflowStepResult allMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return allCombinatorDelegate.allMatch(predicate, results);
    }

    @Nonnull
    @Override
    public CombinatorWorkflowStepResult anyMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return anyCombinatorDelegate.anyMatch(predicate, results);
    }

    @Nonnull
    @Override
    public CombinatorWorkflowStepResult noneMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return noneCombinatorDelegate.noneMatch(predicate, results);
    }


    @Nonnull
    public CompletableFuture<Void> publishEvent(
            @Nonnull ProcessingContext processingContext,
            @Nonnull EventMessage eventMessage) {
        return this.eventSink.publish(processingContext, eventMessage);
    }

    @Nonnull
    public Clock clock() {
        return this.clock;
    }

    @Nonnull
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
     * @return the non-transactional unit of work factory used for the workflow-body wrapper.
     */
    @Nonnull
    public UnitOfWorkFactory workflowBodyUnitOfWorkFactory() {
        return this.workflowBodyUnitOfWorkFactory;
    }

    @Nonnull
    public ExecutorService executorService() {
        return this.executorService;
    }

    /**
     * Event dispatching to the "wait for primitive".
     *
     * @param awaited event arrival wrapper object.
     */
    @Internal
    public void eventReceived(@Nonnull EventWaitConditions.Awaited awaited) {
        waitForDelegate.eventReceived(awaited);
    }

    /**
     * Retrieves typed version of the workflow context.
     *
     * @param <T> workflow context type.
     * @return typed workflow context created by the factory.
     */
    public <T extends WorkflowContext> T typedWorkflowContext() {
        try {
            //noinspection unchecked
            return (T) this.workflowContext;
        } catch (ClassCastException e) {
            throw new WorkflowFailedException("Failed to cast workflow context", e);
        }
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("workflowId", workflowExecution.workflowId());
        descriptor.describeProperty("workflowName", workflowExecution.workflowName());
    }
}
