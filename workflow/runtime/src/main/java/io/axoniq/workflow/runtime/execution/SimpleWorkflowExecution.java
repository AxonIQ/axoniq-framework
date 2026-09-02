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

import io.axoniq.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionCheckpointingSupport.ExecutionTaskQueue;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import org.axonframework.common.ExceptionUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;
import static io.axoniq.workflow.runtime.util.FutureResolver.resolve;
import static java.lang.Thread.currentThread;

/**
 * Workflow instance implementation.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 0.1.0
 */
public final class SimpleWorkflowExecution implements WorkflowExecution, WorkflowCancellationProvider {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWorkflowExecution.class);

    // State variables
    private volatile EventSourcedWorkflowState workflowState;
    // Attributes
    private final String workflowId;
    private final String workflowName;
    private final WorkflowConfiguration<?> workflowConfiguration;

    // Execution
    private final WorkflowContextDelegation contextDelegate;

    // Runtime
    private boolean running = false;
    private boolean stoppedForRecovery = false;
    private volatile Thread workflowThread;
    private final BlockingQueue<Consumer<WorkflowExecution>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
    private final EventWaitConditions eventWaitConditions;
    private final RunningSteps runningSteps;
    private final ReachedSteps reachedSteps;
    private final WorkflowTerminalTransition terminalTransition = this::transitionToTerminalState;
    private final WorkflowCancellation.Request workflowCancellationRequest;
    private final WorkflowExecutionCheckpointingSupport checkpointingSupport =
            new WorkflowExecutionCheckpointingSupport(
                    new ExecutionTaskQueue() {
                        @Override
                        public boolean isRunning() {
                            return running;
                        }

                        @Override
                        public boolean hasQueuedTasks() {
                            return !taskQueue.isEmpty();
                        }

                        @Override
                        public void appendTask(Consumer<WorkflowExecution> task) {
                            if (taskQueue.offer(task)) {
                                return;
                            }
                            // whoops, we're overloading this workflow with events. STOP!!!
                            throw new RuntimeException("Too many tasks to perform workflow instance"); // FIXME <- task queue is full, backpressure?
                        }
                    },
                    CheckpointWorkStateListener.NO_OP
            );
    private final SequencedAppender appendCondition = new SequencedAppender();
    private final WorkflowEventPublisher workflowEventPublisher;

    /**
     * Constructs a new instance.
     *
     * @param workflowId            workflow id.
     * @param initial               initial payload of workflow instance.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     * @param workflowContext       workflow context created by the factory.
     */
    public SimpleWorkflowExecution(String workflowId,
                                   Map<String, @Nullable Object> initial,
                                   ProcessingContext processingContext,
                                   WorkflowConfiguration<?> workflowConfiguration,
                                   WorkflowContext workflowContext
    ) {
        this(workflowId,
             initial,
             processingContext,
             workflowConfiguration,
             workflowContext,
             new RunningSteps(),
             new EventWaitConditions(),
             new ReachedSteps());
    }

    SimpleWorkflowExecution(String workflowId,
                            Map<String, @Nullable Object> initial,
                            ProcessingContext processingContext,
                            WorkflowConfiguration<?> workflowConfiguration,
                            WorkflowContext workflowContext,
                            RunningSteps runningSteps,
                            EventWaitConditions eventWaitConditions,
                            ReachedSteps reachedSteps) {
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must not be null");
        this.workflowConfiguration = Objects.requireNonNull(workflowConfiguration,
                                                            "Workflow configuration must not be null");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps must not be null");
        this.eventWaitConditions = Objects.requireNonNull(eventWaitConditions,
                                                          "Event wait conditions must not be null");
        this.reachedSteps = Objects.requireNonNull(reachedSteps, "Reached steps must not be null");
        var configuredName = Objects.requireNonNull(workflowConfiguration.workflowName(),
                                                    "Workflow name must not be null");

        this.workflowName = configuredName.isEmpty() ? workflowId : configuredName;
        var workflowDefinitionId = new MessageType(
                new QualifiedName(configuredName),
                workflowConfiguration.workflowVersion()
        );

        this.contextDelegate = new WorkflowContextDelegation(
                workflowConfiguration,
                workflowContext,
                this,
                runningSteps,
                eventWaitConditions,
                reachedSteps,
                terminalTransition,
                processingContext
        );
        this.workflowEventPublisher = new WorkflowEventPublisher(
                contextDelegate.eventStore(),
                workflowId,
                contextDelegate.unitOfWorkFactory(),
                contextDelegate.executorService()
        );
        this.workflowCancellationRequest = new DefaultWorkflowCancellation(this, contextDelegate, runningSteps);

        initializeState(
                new EventSourcedWorkflowState(
                        workflowId,
                        initial,
                        workflowDefinitionId
                )
        );
    }


    /**
     * {@inheritDoc}
     * <p>
     * The body runs inside a unit of work spanning the instance's entire lifetime. It is created from the
     * non-transactional {@link WorkflowContextDelegation#workflowBodyUnitOfWorkFactory()} so that no transactional
     * resources are held while the instance is parked.
     */
    @Override
    public CompletableFuture<Void> execute(Consumer<WorkflowExecution> terminationHandler) {
        this.stoppedForRecovery = false;
        this.running = true;
        checkpointingSupport.refreshCheckpointWorkState();
        // run in a separate thread to avoid blocking the replay status change handler thread ( = WorkPackage)

        return ProcessingContextUtils
                .executeWithResultInSeparateThread(
                        contextDelegate.workflowId(),
                        contextDelegate.workflowBodyUnitOfWorkFactory(),
                        contextDelegate.executorService(),
                        this.processingContext(),
                        ctx -> {
                            workflowThread = currentThread();
                            if (workflowCancellationRequest.hasPendingWorkflowCancellation()) {
                                workflowThread.interrupt();
                            }

                            if (this.state().workflowStatus().isTerminal()) {
                                return skipTerminalInstance();
                            }
                            logger.trace("Thread: {}, ProcessingContext {}", currentThread(), ctx);

                            var publicationResolutionTimedOut = false;
                            try {
                                publishStartWorkflow(ctx);
                                executeWorkflow(ctx);
                            } catch (FutureResolutionTimeoutException timeout) {
                                publicationResolutionTimedOut = true;
                                logPublicationResolutionTimeout(timeout);
                                throw timeout;
                            } catch (Throwable e) {
                                try {
                                    handleWorkflowException(ctx, e);
                                } catch (FutureResolutionTimeoutException timeout) {
                                    publicationResolutionTimedOut = true;
                                    logPublicationResolutionTimeout(timeout);
                                    throw timeout;
                                }
                            } finally {
                                if (!publicationResolutionTimedOut) {
                                    finishWorkflow(terminationHandler);
                                } else {
                                    stopRuntimeForRecovery();
                                }
                            }
                            return CompletableFuture.completedFuture(null);
                        }
                )
                .thenApply(ignored -> null);
    }


    /**
     * Skips the body of an instance that is already terminal.
     *
     * @return the context of this instance, as the result of an execution that ran no body
     */
    private CompletableFuture<WorkflowContextDelegation> skipTerminalInstance() {
        logger.trace("Workflow instance has reached terminal state {}, skipping execution.",
                     this.state().workflowStatus());
        return CompletableFuture.completedFuture(this.contextDelegate);
    }

    /**
     * Emit start events if not already started.
     *
     * @param ctx processing context.
     */
    private void publishStartWorkflow(ProcessingContext ctx) {
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        if (this.state().workflowStatus() == WorkflowStatus.NONE) {
            publishAndWait(startedWorkflow(this.workflowContext(), workflowName,
                                           workflowState.workflowDefinitionId(), eventNameCustomizer), ctx);
            try {
                awaitStateChange(s -> s.workflowStatus() == WorkflowStatus.STARTED);
            } catch (Exception e) {
                logger.error("Error waiting for start of workflow instance {}", workflowId, e);
            }
        }
    }

    /**
     * Executes the workflow.
     *
     * @param ctx processing context.
     * @throws Exception if something went wrong.
     */
    private void executeWorkflow(ProcessingContext ctx) throws Exception {
        var workflowPayload = this.workflowContext().workflowPayload();
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        logger.info("Executing workflow with initial payload {} from thread {}",
                    workflowPayload,
                    currentThread());

        // Reset the runtime "book" - step-reference tracker for the drift guard.
        this.reachedSteps.clear();

        // Dispatch to the definition matching state.workflowDefinitionId().version().
        var definition = WorkflowConfigurationRegistry.resolveOrFallback(
                ctx, workflowName, workflowId, this.state().workflowDefinitionId().version(), this.workflowConfiguration
        ).workflowDefinition();
        definition.accept(this.contextDelegate.typedWorkflowContext());

        if (completeCancellationRequest(ctx)) {
            return;
        }
        if (!this.state().workflowStatus().isTerminal()) {
            terminalTransition.transition(() -> {
                publishAndWait(completedWorkflow(this.workflowContext(), workflowName,
                                                 workflowState.workflowDefinitionId(), eventNameCustomizer), ctx);
            });
        }
        logger.info("Workflow executed. Resulting workflow payload {}.", this.workflowContext().workflowPayload());
    }

    /**
     * Handles exceptions thrown during workflow execution.
     * <p>
     * This method applies the workflow exception policy and may publish terminal events. A
     * {@link FutureResolutionTimeoutException} raised by one of those publication attempts propagates to the workflow
     * driver, which stops runtime execution without publishing another terminal event.
     *
     * @param ctx       processing context.
     * @param exception exception to handle.
     */
    private void handleWorkflowException(ProcessingContext ctx, Throwable exception) {
        if (completeCancellationRequest(ctx)) {
            return;
        }
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        switch (exception) {
            case Throwable fenced when isRejected(fenced) -> {
                logger.debug("Workflow {} stopped after an append rejection because another writer already recorded "
                                     + "the event this execution tried to append.", workflowId());
            }
            case WorkflowFailedException wfe -> {
                // if events are already sent by WorkflowLifecycleControlDelegate, just let it propagate
                if (!this.state().workflowStatus().isTerminal()) {
                    terminalTransition.transition(() -> {
                        publishAndWait(failedWorkflow(this.workflowContext(), workflowName, wfe,
                                                     workflowState.workflowDefinitionId(), eventNameCustomizer), ctx);
                    });
                }
            }
            case WorkflowCancelledException wce -> {
                // if events are already sent by WorkflowLifecycleControlDelegate, just let it propagate
                if (!this.state().workflowStatus().isTerminal()) {
                    terminalTransition.transition(() -> {
                        publishAndWait(cancelledWorkflow(this.workflowContext(), workflowName, wce,
                                                        workflowState.workflowDefinitionId(), eventNameCustomizer), ctx);
                    });
                }
            }
            case TimeoutException te -> {
                if (!this.state().workflowStatus().isTerminal()) {
                    terminalTransition.transition(() -> {
                        publishAndWait(timeoutWorkflow(this.workflowContext(), workflowName,
                                                      contextDelegate.clock().instant(),
                                                      workflowState.workflowDefinitionId(), eventNameCustomizer), ctx);
                    });
                }
            }
            case WorkflowReplayDriftException drift -> {
                logger.warn("Workflow {} paused due to replay drift: {}. "
                                    + "Revert the code change or wrap it in ctx.migrateVersion() and replay.",
                            workflowId, drift.getMessage());
                // Intentionally do NOT publish failedWorkflow / cancelledWorkflow events.
                // The workflow stays in its current (non-terminal) state; the next replay will try
                // again. If the developer reverts the offending code or adds ctx.migrateVersion(...), the
                // replay will run cleanly and the workflow continues normally.
            }
            case InterruptedException ie -> {
                Thread.currentThread().interrupt();
            }
            default -> {
                logger.error("Error occurred in workflow {}", workflowId, exception);
            }
        }
    }

    private void logPublicationResolutionTimeout(FutureResolutionTimeoutException timeout) {
        logger.error("Stopping runtime execution for workflow {} because a durable publication did not complete before "
                             + "the resolution timeout. The workflow remains non-terminal and must be recovered from "
                             + "durable history after the processing node restarts. Alert on this error and restart the "
                             + "processing node that owns the workflow.",
                     workflowId,
                     timeout);
    }

    /**
     * Completes a pending cancellation request registered by the cancellation coordinator.
     * <p>
     * This method is called only by the workflow driver after it has been woken from a blocking workflow operation. It
     * consumes the request, clears the wake-up interrupt, and publishes the durable workflow cancellation event on the
     * driver thread. Completing the request future after the terminal transition ensures callers observe a terminal
     * workflow state before their cancellation future completes.
     *
     * @param ctx processing context used to publish the terminal event
     * @return {@code true} when a cancellation request was completed, otherwise {@code false}
     */
    private boolean completeCancellationRequest(ProcessingContext ctx) {
        var cancellation = workflowCancellationRequest.consumeWorkflowCancellation();
        if (cancellation == null) {
            return false;
        }
        Thread.interrupted();
        try {
            terminalTransition.transition(() -> {
                publishAndWait(cancelledWorkflow(this.workflowContext(), workflowName, cancellation.cause(),
                                                 workflowState.workflowDefinitionId(),
                                                 workflowConfiguration.eventNameCustomizer()), ctx);
            });
        } catch (Throwable failure) {
            cancellation.callback().completeExceptionally(failure);
            throwUnchecked(failure);
        }
        cancellation.callback().complete(null);
        return true;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }


    /**
     * Finish the workflow execution, clean up everything, and call the termination handler.
     *
     * @param terminationHandler termination handler to call.
     */
    private void finishWorkflow(Consumer<WorkflowExecution> terminationHandler) {
        stopRuntime(null);
        terminationHandler.accept(this);
    }

    /**
     * Stops the in-memory workflow driver after a durable publication cannot be resolved.
     * <p>
     * The execution deliberately remains in the repository and non-terminal so a processing-node restart can restore it
     * from durable history. In contrast to {@link #finishWorkflow(Consumer)}, this method must not invoke the
     * termination handler because that handler removes the execution from the engine.
     */
    private void stopRuntimeForRecovery() {
        this.stoppedForRecovery = true;
        stopRuntime(new StepInterruptedException("Workflow runtime stopped after publication resolution timeout"));
    }

    /**
     * Releases resources owned by the live workflow driver.
     * <p>
     * This cleanup is common to terminal completion and recovery stop. In both cases no driver remains to drain the
     * task queue, so pending checkpoint latches must be completed before the execution is left in its final in-memory
     * state.
     */
    private void stopRuntime(@Nullable Throwable stepCancellationCause) {
        this.running = false;
        this.taskQueue.clear();
        // Queue cleanup removes checkpoint barriers too. Release their callbacks because no workflow driver remains to
        // consume them; otherwise a fully deferred processor checkpoint would wait forever.
        this.checkpointingSupport.completePendingCheckpointLatch();
        this.eventWaitConditions.clear();
        this.runningSteps.cancelAll(stepCancellationCause, s -> {
        });
        this.checkpointingSupport.refreshCheckpointWorkState();
    }

    private void transitionToTerminalState(Runnable terminalEventPublication) {
        runningSteps.cancelAll(new StepInterruptedException("Workflow reached terminal state"), cancelled -> {
        });
        // Keep checkpoint barriers until finishWorkflow can release their callbacks after the terminal event is durable.
        this.taskQueue.removeIf(task -> !checkpointingSupport.isCheckpointLatch(task));
        terminalEventPublication.run();
        try {
            awaitStateChange(s -> s.workflowStatus().isTerminal());
        } catch (Exception e) {
            logger.error("Error waiting for termination of workflow instance {}", workflowId, e);
        }
    }

    @Override
    public void awaitStateChange(
            Predicate<WorkflowState> predicate
    ) throws InterruptedException {
        do {
            var taken = taskQueue.take();
            checkpointingSupport.runTask(taken, this);
        } while (!predicate.test(this.state()));
    }

    @Override
    public void onEvent(EventMessage eventMessage, ProcessingContext processingContext) {
        if (running) {
            // live mode
            appendTask(i -> {
                eventWaitConditions.evaluateAndApply(eventMessage, processingContext, contextDelegate::eventReceived);
                workflowState.evolve(eventMessage, processingContext);
            });
        } else if (stoppedForRecovery) {
            // The driver was deliberately stopped after a publication-resolution timeout. Keep its projected state in
            // sync with durable events, but do not queue work that no driver can consume before the required restart.
            workflowState.evolve(eventMessage, processingContext, false);
        } else {
            // replay mode
            workflowState.evolve(eventMessage, processingContext, false);
            // The wake must not be discarded with them: an event that produced no engine event before the crash is not
            // in the durable state, so evolving alone leaves the instance waiting for something already gone past.
            appendTask(i -> eventWaitConditions.evaluateAndApply(eventMessage,
                                                                 processingContext,
                                                                 contextDelegate::eventReceived));
        }
    }


    @Override
    public void stopForShutdown() {
        runningSteps.cancelAll(new StepInterruptedException("Workflow engine shutdown"), s -> {
        });
        // Unblock the workflow driver thread parked on taskQueue.take() inside the current step's await() loop.
        // The task sets the driver thread's interrupt flag; the next taskQueue.take() observes it and throws
        // InterruptedException, propagating up so the driver thread exits cleanly.
        taskQueue.offer(i -> Thread.currentThread().interrupt());
    }

    @Override
    public void interruptWorkflowDriver() {
        var driver = workflowThread;
        if (driver != null) {
            driver.interrupt();
        }
    }

    @Override
    public void addCheckpointLatch(Runnable latch) {
        checkpointingSupport.addCheckpointLatch(latch);
    }

    @Override
    public boolean hasUnsafeCheckpointWork() {
        return checkpointingSupport.hasUnsafeCheckpointWork();
    }

    @Override
    public void registerCheckpointWorkStateListener(CheckpointWorkStateListener listener) {
        checkpointingSupport.registerListener(listener);
    }

    @Override
    @Nullable
    public Consumer<WorkflowExecution> getNextTask() {
        var task = this.taskQueue.poll();
        return task == null ? null : execution -> checkpointingSupport.runTask(task, execution);
    }

    @Override
    public void appendTask(Consumer<WorkflowExecution> task) {
        checkpointingSupport.appendTask(task);
    }

    @Override
    public boolean hasTasks() {
        return !this.taskQueue.isEmpty();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public CompletableFuture<Void> appendWorkflowEvent(EventMessage eventMessage, Context parentContext) {
        return appendCondition.appendSequentially(marker -> workflowEventPublisher.publish(
                eventMessage,
                parentContext,
                appendConditionFor(marker)
        )).whenComplete((ignored, failure) -> {
            if (isRejected(failure)) {
                logger.warn("Append of {} for workflow '{}' was rejected: another writer already recorded events for "
                                    + "this instance. Stopping this execution.",
                            eventMessage.type(), workflowId);
                interruptWorkflowDriver();
            }
        });
    }

    boolean isRejected(Throwable failure) {
        return ExceptionUtils.findException(failure, AppendEventsTransactionRejectedException.class).isPresent();
    }

    private AppendCondition appendConditionFor(@Nullable ConsistencyMarker marker) {
        var condition = AppendCondition.withCriteria(EventSourcedWorkflowState.criteriaBuilder(workflowId));
        return marker == null ? condition : condition.withMarker(marker);
    }

    /**
     * Appends a workflow-owned event and waits for its durable publication through the context's future resolver.
     *
     * @param event event to append
     * @param context context used for the append and its resolution policy
     */
    private void publishAndWait(EventMessage event, ProcessingContext context) {
        resolve(context, appendWorkflowEvent(event, context));
    }

    @Override
    public ProcessingContext processingContext() {
        return contextDelegate.processingContext();
    }

    @Override
    public WorkflowState state() {
        return workflowState;
    }

    @Override
    public WorkflowContext workflowContext() {
        return this.contextDelegate;
    }

    @Override
    public String workflowName() {
        return this.workflowName;
    }

    @Override
    public String workflowId() {
        return this.workflowId;
    }

    @Override
    public WorkflowConfiguration<?> workflowConfiguration() {
        return this.workflowConfiguration;
    }

    @Override
    public void initializeState(WorkflowState state) {
        this.workflowState = new EventSourcedWorkflowState(
                state,
                this.contextDelegate.typedWorkflowContext(),
                this.workflowConfiguration.workflowStatusChangeListeners()
        );
    }

    @Override
    public void restoreAppendPosition(@Nullable ConsistencyMarker position) {
        appendCondition.updateMarker(position);
    }

    @Override
    public WorkflowCancellation workflowCancellation() {
        return workflowCancellationRequest;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", contextDelegate);
        descriptor.describeProperty("running", running);
        descriptor.describeProperty("state", state());
        eventWaitConditions.describeTo(descriptor);
        runningSteps.describeTo(descriptor);
        reachedSteps.describeTo(descriptor);
    }
}
