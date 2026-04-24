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

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.TerminatePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static io.axoniq.workflow.configuration.WorkflowConfigurerDefaults.WORKFLOW_ENGINE_EXECUTOR;
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
    private final TerminateDelegate terminateDelegate;
    private final PayloadDelegate payloadDelegate;

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
    private final Clock clock;
    private final ExecutorService executorService;


    /**
     * Creates the context delegation.
     *
     * @param workflowContext   workflow context created by the factory.
     * @param workflowExecution workflow execution.
     * @param processingContext processing context.
     */
    public WorkflowContextDelegation(
            @Nonnull WorkflowConfiguration<?> workflowConfiguration,
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull ProcessingContext processingContext
    ) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution,
                                                        "Workflow execution supplier must not be null");

        var stepParent = workflowConfiguration.eventNameCustomizer().forStepInheritance();

        this.processingContext = Objects.requireNonNull(processingContext, "Processing context must not be null");
        this.workflowContext = workflowContext;

        this.unitOfWorkFactory = Objects.requireNonNull(processingContext.component(UnitOfWorkFactory.class),
                                                        "Could not retrieve UoW factory");
        this.clock = Objects.requireNonNull(processingContext.component(Clock.class), "Could not retrieve Clock");
        this.executorService = Objects.requireNonNull(processingContext.component(ExecutorService.class,
                                                                                  WORKFLOW_ENGINE_EXECUTOR),
                                                      "Could not retrieve workflow engine executor");
        this.eventSink = Objects.requireNonNull(processingContext.component(EventSink.class),
                                                "Could not retrieve EventSink");


        var executeDelegate = new ExecuteDelegate(workflowContext,
                                                  workflowExecution,
                                                  stepParent,
                                                  clock,
                                                  unitOfWorkFactory,
                                                  eventSink,
                                                  executorService);
        this.retryableExecuteDelegate = new RetryableExecuteDelegate(executeDelegate,
                                                                     workflowContext,
                                                                     workflowExecution,
                                                                     stepParent,
                                                                     clock,
                                                                     unitOfWorkFactory,
                                                                     eventSink,
                                                                     executorService);
        this.waitForDelegate = new WaitForDelegate(workflowContext,
                                                   workflowExecution,
                                                   stepParent,
                                                   clock,
                                                   unitOfWorkFactory,
                                                   eventSink,
                                                   executorService);
        this.terminateDelegate = new TerminateDelegate(workflowContext,
                                                       workflowExecution,
                                                       unitOfWorkFactory,
                                                       eventSink,
                                                       executorService);
        this.payloadDelegate = new PayloadDelegate(workflowContext,
                                                   workflowExecution,
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
    public Map<String, Object> workflowPayload() {
        return workflowExecution.state().payload();
    }

    @Override
    public void modifyPayload(@Nonnull String stepName,
                              @Nonnull PayloadModification payloadModification,
                              @Nonnull EventNameCustomizer eventNameCustomizer) {
        payloadDelegate.modifyPayload(stepName, payloadModification, eventNameCustomizer);
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
    public WorkflowStepResult execute(@Nonnull String stepName,
                                      @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterPayloadReducer,
                                      @Nonnull PayloadReducer resultPayloadReducer, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        workflowExecution.state().throwTerminalCause();
        return retryableExecuteDelegate.execute(stepName,
                                                local,
                                                action,
                                                parameterPayloadReducer,
                                                resultPayloadReducer,
                                                timeout,
                                                eventNameCustomizer);
    }

    @Override
    @Nonnull
    public WorkflowStepResult execute(@Nonnull String stepName,
                                      @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterPayloadReducer,
                                      @Nonnull PayloadReducer resultPayloadReducer, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer,
                                      @Nonnull RetryPolicy retryPolicy) {
        workflowExecution.state().throwTerminalCause();
        return retryableExecuteDelegate.execute(stepName,
                                                local,
                                                action,
                                                parameterPayloadReducer,
                                                resultPayloadReducer,
                                                timeout,
                                                eventNameCustomizer,
                                                retryPolicy);
    }

    @Override
    @Nonnull
    public WorkflowStepResult waitFor(@Nonnull String stepName,
                                      @Nonnull EventCondition eventCondition,
                                      @Nonnull PayloadReducer resultPayloadReducer,
                                      @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        workflowExecution.state().throwTerminalCause();
        return waitForDelegate.waitFor(stepName, eventCondition, resultPayloadReducer, timeout, eventNameCustomizer);
    }

    @Override
    public void terminate(@Nonnull TerminatePrimitive.TerminateCommand command) {
        if (command.isStepCancellation()) {
            terminateDelegate.terminate(command);
            return;
        }
        workflowExecution.state().throwTerminalCause();
        terminateDelegate.terminate(new TerminatePrimitive.TerminateCommand(
                command.error(),
                command.cause(),
                merge(workflowExecution.workflowConfiguration().eventNameCustomizer(),
                      command.eventNameCustomizer()),
                workflowExecution.workflowName(),
                null
        ));
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
    public <T extends WorkflowContext> T typepWorkflowContext() {
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
