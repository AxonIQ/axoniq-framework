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
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.engine.execution.EventSourcedWorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
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
import java.util.Objects;
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
    // Attributes
    private final String workflowId;
    private final String workflowName;
    private final WorkflowConfiguration<?> workflowConfiguration;

    // Execution
    private final WorkflowContextDelegation contextDelegate;

    // Runtime
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
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must not be null");
        this.workflowConfiguration = Objects.requireNonNull(workflowConfiguration,
                                                            "Workflow configuration must not be null");
        var configuredName = Objects.requireNonNull(workflowConfiguration.workflowName(),
                                                    "Workflow name must not be null");
        this.workflowName = configuredName.isEmpty() ? workflowId : configuredName; // FIXME


        this.contextDelegate = new WorkflowContextDelegation(
                workflowConfiguration,
                workflowContext,
                this,
                processingContext
        );
        this.workflowState = new EventSourcedWorkflowState(
                initial,
                this.contextDelegate.typepWorkflowContext(),
                workflowConfiguration.workflowStatusChangeListeners()
        );
    }


    @Override
    public void execute(@Nonnull Consumer<WorkflowExecution> terminationHandler) {
        // TODO: discuss when we switch to the executable
        this.executable = true;
        ProcessingContextUtils
                .executeWithResult(
                        contextDelegate.workflowId(),
                        contextDelegate.unitOfWorkFactory(),
                        contextDelegate.executor(),
                        this.processingContext(),
                        ctx -> {
                            logger.trace("Thread: {}, ProcessingContext {}", Thread.currentThread(), ctx);
                            var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
                            if (this.state().workflowStatus().isTerminal()) {
                                logger.info("Workflow instance has reached terminal state {}, skipping execution.",
                                            this.state().workflowStatus());
                                return CompletableFuture.completedFuture(this.contextDelegate);
                            }

                            if (this.state().workflowStatus() == WorkflowStatus.NONE) {
                                sendWorkflowEvent(startedWorkflow(this.workflowContext(),
                                                                  workflowName,
                                                                  eventNameCustomizer),
                                                  ctx)
                                        .join(); // FIXME join without timeout?
                            }

                            try {
                                logger.info("Executing workflow with initial payload {} from thread {}",
                                            this.workflowContext().workflowPayload(),
                                            Thread.currentThread());

                                this.workflowConfiguration.workflowDefinition()
                                                          .accept(this.contextDelegate.typepWorkflowContext());
                                logger.info("Workflow executed. Resulting workflow payload {}.",
                                            this.workflowContext().workflowPayload());

                                if (!this.state().workflowStatus().isTerminal()) {
                                    sendWorkflowEvent(completedWorkflow(this.workflowContext(),
                                                                        workflowName,
                                                                        eventNameCustomizer),
                                                      ctx).get(
                                            5,
                                            TimeUnit.SECONDS); // FIXME constant?
                                }
                            } catch (WorkflowFailedException e) {
                                // if Events are already sent by TerminateDelegate, just let it propagate
                                if (!this.state().workflowStatus().isTerminal()) {
                                    sendWorkflowEvent(failedWorkflow(this.workflowContext(),
                                                                     workflowName,
                                                                     e,
                                                                     eventNameCustomizer),
                                                      ctx).join(); // FIXME join without timeout
                                }
                            } catch (WorkflowCancelledException e) {
                                // if Events are already sent by TerminateDelegate, just let it propagate
                                if (!this.state().workflowStatus().isTerminal()) {
                                    sendWorkflowEvent(cancelledWorkflow(this.workflowContext(),
                                                                        workflowName,
                                                                        e,
                                                                        eventNameCustomizer),
                                                      ctx).join(); // FIXME join without timeout
                                }
                            } catch (Throwable e) {
                                if (e instanceof TimeoutException) {
                                    sendWorkflowEvent(timeoutWorkflow(this.workflowContext(),
                                                                      workflowName,
                                                                      contextDelegate.clock().instant(),
                                                                      eventNameCustomizer),
                                                      ctx).join(); // FIXME join without timeout
                                } else if (e instanceof InterruptedException) {
                                    sendWorkflowEvent(cancelledWorkflow(this.workflowContext(),
                                                                        workflowName,
                                                                        eventNameCustomizer),
                                                      ctx).join(); // FIXME join without timeout
                                } else {
                                    logger.error("Error occurred in workflow {}", workflowId, e);
                                    sendWorkflowEvent(failedWorkflow(this.workflowContext(),
                                                                     workflowName,
                                                                     e instanceof Exception ? (Exception) e
                                                                             : new RuntimeException(e),
                                                                     eventNameCustomizer),
                                                      ctx).join(); // FIXME join without timeout
                                }
                            }

                            return CompletableFuture.completedFuture(this.contextDelegate);
                        }
                ).handle((wc, te) -> {
                    try {
                        awaitStateChange(s -> s.workflowStatus().isTerminal());
                    } catch (Exception e) {
                        logger.error(
                                "Error waiting for termination of workflow instance {}", workflowId,
                                e
                        );
                    }
                    cleanup();
                    terminationHandler.accept(this);
                    return null;
                })
                .join();
    }

    /**
     * Performs internal cleanup of the execution. Everything related to the execution is removed, and only the
     * execution state remains present.
     */
    private void cleanup() {
        this.taskQueue.clear();
        this.eventWaitConditions.clear();
        this.runningSteps.cancelAll(null, s -> {
        });
    }

    @Override
    public void awaitStateChange(
            @Nonnull Predicate<WorkflowState> predicate
    ) throws InterruptedException {
        do {
            var taken = taskQueue.take();
            taken.accept(this);
        } while (!predicate.test(this.state()));
    }

    @Override
    public void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        eventWaitConditions.evaluateAndApply(eventMessage, contextDelegate::eventReceived);
        appendTask(i -> state().evolve(eventMessage, processingContext));
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
    public void cancelRunningStep(@Nonnull String stepName, @Nullable Throwable cause) {
        boolean cancelled = runningSteps.cancelWithCause(stepName, cause);
        if (cancelled) {
            try {
                awaitStateChange(s -> s.containsStep(stepName)
                        && s.getStep(stepName).status().isTerminal());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
                        .allMatch(stepName -> s.containsStep(stepName)
                                && s.getStep(stepName).status().isTerminal()
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
                                      @Nonnull PayloadReducer resultPayloadReducer,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        eventWaitConditions.add(stepName, eventCondition, resultPayloadReducer, eventNameCustomizer);
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

    @Nonnull
    @Override
    public String workflowName() {
        return this.workflowName;
    }

    @Nonnull
    @Override
    public String workflowId() {
        return this.workflowId;
    }

    @Nonnull
    @Override
    public WorkflowConfiguration<?> workflowConfiguration() {
        return this.workflowConfiguration;
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
