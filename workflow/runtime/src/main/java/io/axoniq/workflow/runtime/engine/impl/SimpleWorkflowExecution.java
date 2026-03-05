/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.engine.execution.SimpleWorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateProjector;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;

/**
 * Workflow instance implementation.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
public final class SimpleWorkflowExecution implements WorkflowExecution {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWorkflowExecution.class);

    // State variables
    private final WorkflowState workflowState;

    // Runtime
    private final WorkflowContextDelegation contextDelegate;
    private final BlockingQueue<Consumer<WorkflowExecution>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
    private final EventWaitConditions eventWaitConditions = new EventWaitConditions();
    private final RunningSteps runningSteps = new RunningSteps();

    private boolean executable = false;

    /**
     * Constructs a new instance.
     *
     * @param workflowId            workflow id.
     * @param initial               initial payload of workflow instance.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     * @param workflowContext       workflow context created by the factory.
     */
    public SimpleWorkflowExecution(@Nonnull String workflowId,
                                   @Nonnull Map<String, Object> initial,
                                   @Nonnull ProcessingContext processingContext,
                                   @Nonnull WorkflowConfiguration<?> workflowConfiguration,
                                   @Nonnull WorkflowContext workflowContext
    ) {
        this.contextDelegate = new WorkflowContextDelegation(
                workflowId,
                initial,
                workflowConfiguration,
                workflowContext,
                () -> this,
                processingContext
        );
        this.workflowState = new SimpleWorkflowState(this.contextDelegate.typepWorkflowContext(),
                                                     workflowConfiguration.workflowStatusChangeListeners());
    }


    @Override
    @Nonnull
    public <T extends WorkflowContext> T execute() {
        // TODO: discuss when we switch to the executable
        this.executable = true;
        //noinspection unchecked
        return (T) ProcessingContextUtils
                .executeWithResult(
                        contextDelegate.workflowId(),
                        contextDelegate.unitOfWorkFactory(),
                        contextDelegate.executor(),
                        this.processingContext(),
                        pc -> {

                            var workflowName = this.contextDelegate.workflowName();
                            var eventNameCustomizer = this.contextDelegate.workflowConfiguration().eventNameCustomizer();
                            if (this.state().workflowStatus().isTerminal()) {
                                logger.trace("Workflow instance has reached terminal state {}, skipping execution.",
                                             this.state().workflowStatus());
                                return CompletableFuture.completedFuture(this);
                            }

                            if (this.state().workflowStatus() == WorkflowStatus.NONE) {
                                sendWorkflowEvent(startedWorkflow(this.workflowContext(),
                                                                  workflowName,
                                                                  eventNameCustomizer),
                                                  pc).join(); // FIXME join without timeout?
                            }

                            try {
                                logger.trace("Executing workflow with initial payload {} from thread {}",
                                             this.workflowContext().workflowPayload(),
                                             Thread.currentThread());

                                this.contextDelegate.workflowConfiguration().workflowDefinition()
                                                    .accept(this.contextDelegate.typepWorkflowContext());
                                logger.trace("Workflow executed. Resulting workflow payload {}.",
                                             this.workflowContext().workflowPayload());

                                if (!this.state().workflowStatus().isTerminal()) {
                                    sendWorkflowEvent(completedWorkflow(this.workflowContext(),
                                                                        workflowName,
                                                                        eventNameCustomizer),
                                                      pc).get(
                                            5,
                                            TimeUnit.SECONDS); // FIXME constant?
                                }
                            } catch (WorkflowFailedException | WorkflowCancelledException e) {
                                // Events already sent by TerminateDelegate, just let it propagate
                            } catch (Throwable e) {
                                if (e instanceof TimeoutException) {
                                    sendWorkflowEvent(timeoutWorkflow(this.workflowContext(),
                                                                      workflowName,
                                                                      contextDelegate.clock().instant(),
                                                                      eventNameCustomizer),
                                                      pc).join(); // FIXME join without timeout
                                } else if (e instanceof InterruptedException) {
                                    sendWorkflowEvent(cancelledWorkflow(this.workflowContext(),
                                                                        workflowName,
                                                                        eventNameCustomizer),
                                                      pc).join(); // FIXME join without timeout
                                } else {
                                    logger.error("Error occurred in workflow {}", this.contextDelegate.workflowId(), e);
                                    sendWorkflowEvent(failedWorkflow(this.workflowContext(),
                                                                     workflowName,
                                                                     e instanceof Exception ? (Exception) e
                                                                             : new RuntimeException(e),
                                                                     eventNameCustomizer),
                                                      pc).join(); // FIXME join without timeout
                                }
                                if (e instanceof RuntimeException) {
                                    throw (RuntimeException) e;
                                } else {
                                    throw new RuntimeException(e);
                                }
                            }

                            return CompletableFuture.completedFuture(this.contextDelegate);
                        }
                ).thenApply(wc -> {
                    try {
                        // FIXME -> tell the coordinator to clean up and wait for terminal workflow status.
                        awaitStateChange(s -> s.state().workflowStatus().isTerminal());
                    } catch (Exception te) {
                        logger.error("Error waiting for workflow instance termination", te);
                    }
                    return wc;
                })
                .join();
    }

    @Override
    public void awaitStateChange(
            @Nonnull Predicate<WorkflowExecution> predicate
    ) throws InterruptedException {
        do {
            taskQueue.take().accept(this);
        } while (!predicate.test(this));
    }

    @Override
    public void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        eventWaitConditions.evaluateAndApply(eventMessage, contextDelegate::eventReceived);
        appendTask(i -> WorkflowStateProjector.applyStateChange(eventMessage, processingContext, state()));
    }

    @Override
    public void registerRunningStep(@Nonnull String stepName, @Nonnull CompletableFuture<?> future) {
        runningSteps.register(stepName, future);
    }

    @Override
    public void removeRunningStep(@Nonnull String stepName) {
        runningSteps.remove(stepName);
    }

    @Override
    public boolean cancelRunningStep(@Nonnull String stepName, @Nullable Throwable cause) {
        boolean cancelled = runningSteps.cancelWithCause(stepName, cause);
        if (cancelled) {
            try {
                awaitStateChange(s -> s.state().containsStep(stepName)
                        && s.state().getStep(stepName).status().isTerminal());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return cancelled;
    }

    @Override
    public void cancelAllRunningSteps(@Nullable Throwable cause) {
        runningSteps.cancelAll(cause, cancelledSteps -> {
            if (cancelledSteps.isEmpty()) {
                return;
            }
            try {
                awaitStateChange(s -> cancelledSteps
                        .stream()
                        .allMatch(stepName -> s.state().containsStep(stepName)
                                && s.state().getStep(stepName).status().isTerminal()
                        )
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    public void cancelAndRemoveRunningStep(@Nonnull String stepName, boolean mayInterruptIfRunning) {
        runningSteps.cancelAndRemove(stepName, mayInterruptIfRunning);
    }

    @Override
    @Nullable
    public Consumer<WorkflowExecution> getNextTask() {
        return this.taskQueue.poll(); // FIXME: forever?
    }

    @Override
    public void appendTask(@Nonnull Consumer<WorkflowExecution> task) {
        if (!this.taskQueue.offer(task)) {
            // whoops, we're overloading this workflow with events. STOP!!!
            throw new RuntimeException("Too many events for this workflow instance"); // FIXME <- task queue is full, backpressure?
        }
    }

    @Override
    public void registerWaitCondition(@Nonnull String stepName,
                                      @Nonnull EventCondition eventCondition,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        eventWaitConditions.add(stepName, eventCondition, eventNameCustomizer);
    }

    @Override
    public void removeWaitCondition(@Nonnull String stepName) {
        eventWaitConditions.remove(stepName);
    }

    @Override
    public boolean hasTasks() {
        return this.taskQueue.isEmpty();
    }

    @Override
    public boolean isExecutable() {
        return executable;
    }

    private CompletableFuture<Void> sendWorkflowEvent(
            @Nonnull EventMessage eventMessage,
            @Nonnull ProcessingContext processingContext) {
        // TODO: make sure the consistency marker is used
        return contextDelegate.publishEvent(processingContext, eventMessage);
    }

    @Override
    @Nonnull
    public ProcessingContext processingContext() {
        return contextDelegate.processingContext();
    }

    @Override
    @Nonnull
    public WorkflowState state() {
        return workflowState;
    }

    @Override
    @Nonnull
    public WorkflowContext workflowContext() {
        return this.contextDelegate;
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", contextDelegate);
        descriptor.describeProperty("executable", executable);
        descriptor.describeProperty("state", state());
        eventWaitConditions.describeTo(descriptor);
        runningSteps.describeTo(descriptor);
    }
}
