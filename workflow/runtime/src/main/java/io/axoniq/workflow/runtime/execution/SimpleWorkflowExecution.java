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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowDefinitionId;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;
import static java.lang.Thread.currentThread;

/**
 * Workflow instance implementation.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
public final class SimpleWorkflowExecution implements WorkflowExecution, WorkflowStateRehydratable {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWorkflowExecution.class);

    // State variables
    private final EventSourcedWorkflowState workflowState;
    private final WorkflowConfiguration<?> workflowConfiguration;

    // Execution
    private final WorkflowContextDelegation contextDelegate;

    // Runtime
    private final BlockingQueue<Consumer<WorkflowExecution>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
    private final EventWaitConditions eventWaitConditions = new EventWaitConditions();
    private final RunningSteps runningSteps = new RunningSteps();
    private final Set<String> referencedStepNames = ConcurrentHashMap.newKeySet();
    private final WorkflowExecutionCheckpointSupport checkpointSupport = new WorkflowExecutionCheckpointSupport(new WorkflowExecutionCheckpointSupport.Host() {
        @Override
        public boolean isExecutable() {
            return executable;
        }

        @Override
        public boolean hasQueuedTasks() {
            return !taskQueue.isEmpty();
        }

        @Override
        public void appendTask(@Nonnull Consumer<WorkflowExecution> task) {
            SimpleWorkflowExecution.this.appendTask(task);
        }
    });
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
        this.workflowConfiguration = Objects.requireNonNull(workflowConfiguration,
                                                            "Workflow configuration must not be null");
        var workflowDefinitionId = new WorkflowDefinitionId(
                new QualifiedName(workflowConfiguration.workflowName()),
                workflowConfiguration.workflowVersion()
        );
        this.contextDelegate = new WorkflowContextDelegation(
                workflowConfiguration,
                workflowContext,
                this,
                processingContext
        );
        this.workflowState = new EventSourcedWorkflowState(
                Objects.requireNonNull(workflowId, "Workflow id must not be null"),
                initial,
                workflowDefinitionId,
                this.contextDelegate.typedWorkflowContext(),
                workflowConfiguration.workflowStatusChangeListeners()
        );
    }


    @Override
    public void execute(@Nonnull Consumer<WorkflowExecution> terminationHandler) {
        this.executable = true;
        // run in a separate thread to avoid blocking the replay status change handler thread ( = WorkPackage)

        ProcessingContextUtils
                .executeWithResultInSeparateThread(
                        contextDelegate.workflowId(),
                        contextDelegate.unitOfWorkFactory(),
                        contextDelegate.executorService(),
                        this.processingContext(),
                        ctx -> {

                            if (this.state().workflowStatus().isTerminal()) {
                                logger.trace(
                                        "Workflow instance has reached terminal state {}, skipping execution.",
                                        this.state().workflowStatus()
                                );
                                return CompletableFuture.completedFuture(this.contextDelegate);
                            }
                            logger.trace("Thread: {}, ProcessingContext {}", currentThread(), ctx);

                            publishStartWorkflow(ctx);

                            try {
                                executeWorkflow(ctx);
                            } catch (Throwable e) {
                                handleWorkflowException(ctx, e);
                            }
                            finishWorkflow(terminationHandler);
                            return CompletableFuture.completedFuture(null);
                        }
                );
    }


    /**
     * Emit start events if not already started.
     *
     * @param ctx processing context.
     */
    private void publishStartWorkflow(@Nonnull ProcessingContext ctx) {
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        if (this.state().workflowStatus() == WorkflowStatus.NONE) {
            // FIXME join without timeout?
            sendWorkflowEvent(startedWorkflow(this.workflowContext(),
                                              workflowName(),
                                              workflowState.workflowDefinitionId(),
                                              eventNameCustomizer), ctx).join();
            try {
                awaitStateChange(s -> s.workflowStatus() == WorkflowStatus.STARTED);
            } catch (Exception e) {
                logger.error("Error waiting for start of workflow instance {}", workflowId(), e);
            }
        }
    }

    /**
     * Executes the workflow.
     *
     * @param ctx processing context.
     * @throws Exception if something went wrong.
     */
    private void executeWorkflow(@Nonnull ProcessingContext ctx) throws Exception {
        var workflowPayload = this.workflowContext().workflowPayload();
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        logger.info("Executing workflow with initial payload {} from thread {}",
                    workflowPayload,
                    currentThread());

        // Reset the runtime "book" — step-reference tracker for the drift guard.
        this.referencedStepNames.clear();

        // Dispatch to the definition matching state.workflowDefinitionVersion().
        var definition = WorkflowConfigurationRegistry.resolveOrFallback(
                ctx, workflowName(), workflowId(), this.state().workflowDefinitionVersion(), this.workflowConfiguration
        ).workflowDefinition();
        definition.accept(this.contextDelegate.typedWorkflowContext());

        if (!this.state().workflowStatus().isTerminal()) {
            // Cancel any async steps still running so their CANCELLED events land
            // while the workflow is still non-terminal (sendStepEvent rejects events
            // once the workflow reaches a terminal state).
            cancelAllRunningSteps(new StepCancellationException("Workflow completed"));

            sendWorkflowEvent(
                    completedWorkflow(this.workflowContext(),
                                      workflowName(),
                                      workflowState.workflowDefinitionId(),
                                      eventNameCustomizer),
                    ctx).get(5, TimeUnit.SECONDS); // FIXME constant?
            try {
                awaitStateChange(s -> s.workflowStatus().isTerminal());
            } catch (Exception e) {
                logger.error("Error waiting for completion of workflow instance {}", workflowId(), e);
            }
        }
        logger.info("Workflow executed. Resulting workflow payload {}.", this.workflowContext().workflowPayload());
    }

    /**
     * Handles exceptions thrown during workflow execution.
     *
     * @param ctx       processing context.
     * @param exception exception to handle.
     */
    private void handleWorkflowException(@Nonnull ProcessingContext ctx, @Nonnull Throwable exception) {
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        switch (exception) {
            case WorkflowFailedException wfe -> {
                // if Events are already sent by TerminateDelegate, just let it propagate
                if (!this.state().workflowStatus().isTerminal()) {
                    // Mirror the completion path: cancel running async steps so their
                    // terminal events land before the workflow itself becomes terminal.
                    cancelAllRunningSteps(wfe);
                    sendWorkflowEvent(failedWorkflow(
                                              this.workflowContext(),
                                              workflowName(),
                                              wfe,
                                              workflowState.workflowDefinitionId(),
                                              eventNameCustomizer),
                                      ctx).join(); // FIXME join without timeout
                    try {
                        awaitStateChange(s -> s.workflowStatus().isTerminal());
                    } catch (Exception e) {
                        logger.error("Error waiting for termination of workflow instance {}", workflowId(), e);
                    }
                }
            }
            case WorkflowCancelledException wce -> {
                // if Events are already sent by TerminateDelegate, just let it propagate
                if (!this.state().workflowStatus().isTerminal()) {
                    cancelAllRunningSteps(wce);
                    sendWorkflowEvent(
                            cancelledWorkflow(this.workflowContext(),
                                              workflowName(),
                                              wce,
                                              workflowState.workflowDefinitionId(),
                                              eventNameCustomizer),
                            ctx).join(); // FIXME join without timeout
                    try {
                        awaitStateChange(s -> s.workflowStatus().isTerminal());
                    } catch (Exception e) {
                        logger.error("Error waiting for termination of workflow instance {}", workflowId(), e);
                    }
                }
            }
            case TimeoutException te -> {
                if (!this.state().workflowStatus().isTerminal()) {
                    cancelAllRunningSteps(new StepCancellationException("Workflow timed out"));
                    sendWorkflowEvent(timeoutWorkflow(
                                              this.workflowContext(),
                                              workflowName(),
                                              contextDelegate.clock().instant(),
                                              workflowState.workflowDefinitionId(),
                                              eventNameCustomizer),
                                      ctx).join(); // FIXME join without timeout

                    try {
                        awaitStateChange(s -> s.workflowStatus().isTerminal());
                    } catch (Exception e) {
                        logger.error("Error waiting for termination of workflow instance {}", workflowId(), e);
                    }
                }
            }
            case WorkflowReplayDriftException drift -> {
                logger.warn("Workflow {} paused due to replay drift: {}. "
                                    + "Revert the code change or wrap it in ctx.migrateVersion() and replay.",
                            workflowId(), drift.getMessage());
                // Intentionally do NOT publish failedWorkflow / cancelledWorkflow events.
                // The workflow stays in its current (non-terminal) state; the next replay will try
                // again. If the developer reverts the offending code or adds ctx.migrateVersion(...), the
                // replay will run cleanly and the workflow continues normally.
            }
            case InterruptedException ie -> {
                Thread.currentThread().interrupt();
        /*
                // we agreed not to drive the workflow to terminal state on interrupted exception
                sendWorkflowEvent(
                    cancelledWorkflow(this.workflowContext(),
                                      workflowName,
                                      workflowDefinitionId,
                                      eventNameCustomizer),
                    ctx
                ).join(); // FIXME join without timeout

         */
            }
            default -> {
                logger.error("Error occurred in workflow {}", workflowId(), exception);
                // we agreed not to drive the workflow to terminal state on any other exception
                /*
                sendWorkflowEvent(failedWorkflow(
                                          this.workflowContext(),
                                          workflowName,
                                          exception instanceof Exception ? (Exception) exception : new RuntimeException(exception),
                                          workflowDefinitionId,
                                          eventNameCustomizer),
                                  ctx).join(); // FIXME join without timeout

                 */
            }
        }
    }


    /**
     * Finish the workflow execution, clean up everything, and call the termination handler.
     *
     * @param terminationHandler termination handler to call.
     */
    private void finishWorkflow(Consumer<WorkflowExecution> terminationHandler) {
        this.executable = false; // mark we are done and are not executable anymore
        // TODO -> how do we recognize workflow executions which came to this point bit haven't reach the terminal states?
        this.taskQueue.clear();
        this.eventWaitConditions.clear();
        this.runningSteps.cancelAll(null, s -> {
        });
        terminationHandler.accept(this);
    }


    @Override
    public void awaitStateChange(@Nonnull Predicate<WorkflowState> predicate) throws InterruptedException {
        do {
            var taken = taskQueue.take();
            checkpointSupport.runTask(taken, this);
        } while (!predicate.test(this.state()));
    }

    @Override
    public void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        if (executable) {
            // live mode
            eventWaitConditions.evaluateAndApply(eventMessage, processingContext, contextDelegate::eventReceived);
            appendTask(i -> state().evolve(eventMessage, processingContext));
        } else {
            // replay mode
            state().evolve(eventMessage, processingContext);
        }
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

    /**
     * Cancels the workflow.
     * FIXME as part of https://github.com/AxonIQ/extension-workflow/issues/195
     */
    public void cancel() {
        workflowContext().terminate(
                TerminatePrimitive.TerminateCommand.cancel(null, DefaultEventNameCustomizer.Builder.defaults())
        );
    }

    /**
     * Cancels the workflow with the given reason.
     * FIXME as part of https://github.com/AxonIQ/extension-workflow/issues/195
     *
     * @param reason reason for cancellation
     */
    public void cancel(@NonNull String reason) {
        workflowContext().terminate(
                TerminatePrimitive.TerminateCommand.cancel(new WorkflowCancelledException(reason),
                                                           DefaultEventNameCustomizer.Builder.defaults())
        );
    }

    /**
     * Cancels the workflow with the given cause.
     * FIXME as part of https://github.com/AxonIQ/extension-workflow/issues/195
     *
     * @param cause reason for cancellation
     */
    public void cancel(@NonNull Throwable cause) {
        workflowContext().terminate(
                TerminatePrimitive.TerminateCommand.cancel(cause,
                                                           DefaultEventNameCustomizer.Builder.defaults()));
    }


    @Override
    public void appendCheckpointIntent(@Nonnull Runnable onDrained) {
        checkpointSupport.appendCheckpointIntent(onDrained);
    }

    @Override
    public boolean hasPendingCheckpointWork() {
        return checkpointSupport.hasPendingCheckpointWork();
    }

    @Override
    public void cancelAllRunningSteps(@Nullable Throwable cause) {
        runningSteps.cancelAll(cause, cancelledSteps -> {
            if (cancelledSteps.isEmpty()) {
                return;
            }
            Predicate<WorkflowState> allTerminal = workflowState -> cancelledSteps
                    .stream()
                    .allMatch(stepName -> workflowState.containsStep(stepName)
                            && workflowState.getStep(stepName).status().isTerminal());
            // If every cancelled step is already terminal (e.g., the future had already
            // completed before we requested cancellation), there are no pending tasks
            // to wait for — awaitStateChange would block on an empty task queue.
            if (allTerminal.test(this.state())) {
                return;
            }
            try {
                awaitStateChange(allTerminal);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    public void interrupt() {
        runningSteps.cancelAll(new InterruptedException("Workflow engine shutdown"), s -> {
        });
        // Unblock the workflow driver thread parked on taskQueue.take() inside the current step's await() loop.
        // The task sets the driver thread's interrupt flag; the next taskQueue.take() observes it and throws
        // InterruptedException, propagating up so the driver thread exits cleanly.
        taskQueue.offer(i -> Thread.currentThread().interrupt());
    }

    @Override
    public void cancelAndRemoveRunningStep(@Nonnull String stepName, boolean mayInterruptIfRunning) {
        runningSteps.cancelAndRemove(stepName, mayInterruptIfRunning);
    }

    @Override
    @Nullable
    public Consumer<WorkflowExecution> getNextTask() {
        var task = this.taskQueue.poll(); // FIXME: forever?
        return task == null ? null : ignored -> checkpointSupport.runTask(task, this);
    }

    @Override
    public void appendTask(@Nonnull Consumer<WorkflowExecution> task) {
        if (!this.taskQueue.offer(task)) {
            // whoops, we're overloading this workflow with events. STOP!!!
            throw new RuntimeException("Too many tasks to perform workflow instance"); // FIXME <- task queue is full, backpressure?
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

        return ProcessingContextUtils
                .executeWithResult(
                        UUID.randomUUID().toString(),
                        processingContext.component(UnitOfWorkFactory.class),
                        processingContext.component(Executor.class, WORKFLOW_ENGINE_EXECUTOR),
                        processingContext,
                        childCtx -> {
                            logger.trace("Publishing workflow event {} from {}",
                                         eventMessage.type(),
                                         Thread.currentThread());
                            return contextDelegate.publishEvent(childCtx, eventMessage);
                        }
                );
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
        return workflowState.workflowDefinitionId().qualifiedName().toString();
    }

    @Nonnull
    @Override
    public String workflowId() {
        return workflowState.workflowId();
    }

    @Nonnull
    @Override
    public WorkflowConfiguration<?> workflowConfiguration() {
        return this.workflowConfiguration;
    }

    @Override
    public void rehydrate(@Nonnull EventSourcedWorkflowState state) {
        workflowState.restoreFrom(state);
    }

    @Override
    public void recordStepReference(@Nonnull String stepName) {
        this.referencedStepNames.add(stepName);
    }

    @Override
    @Nonnull
    public Set<String> referencedStepNames() {
        // Live read-only view: reflects ongoing recordStepReference() calls but cannot be mutated by callers.
        return Collections.unmodifiableSet(this.referencedStepNames);
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
