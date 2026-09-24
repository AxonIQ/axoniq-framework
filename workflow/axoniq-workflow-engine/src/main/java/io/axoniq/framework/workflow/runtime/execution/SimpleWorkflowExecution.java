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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.StepInterruptedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowCancelledException;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowFailedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionCheckpointingSupport.ExecutionTaskQueue;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.util.ProcessingContextUtils;
import org.axonframework.common.ExceptionUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.axoniq.framework.workflow.runtime.util.EventMessageUtils.*;
import static io.axoniq.framework.workflow.runtime.util.FutureResolver.resolve;
import static java.lang.Thread.currentThread;

/**
 * Workflow instance implementation.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 5.4.0
 */
public final class SimpleWorkflowExecution implements WorkflowExecution, WorkflowCancellationProvider {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWorkflowExecution.class);
    // Attributes
    private final String workflowId;
    private final String workflowName;
    private final WorkflowConfiguration<?> workflowConfiguration;
    // Execution
    private final WorkflowExecutionOperationsDelegation contextDelegate;
    // Why one of this execution's own appends stopped it. Set by the store callback before the body sees the failure.
    private final AtomicReference<StopReason> stopReason = new AtomicReference<>();
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
    private final SequencedAppender appender = new SequencedAppender();
    private final WorkflowEventPublisher workflowEventPublisher;
    // Runtime
    private boolean running = false;
    private boolean stoppedForRecovery = false;
    private volatile Thread workflowThread;
    // State variables
    private volatile EventSourcedWorkflowState workflowState;

    /**
     * Constructs a new instance.
     *
     * @param workflowId            workflow id.
     * @param initial               initial payload of workflow instance.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     * @param workflowContext       author-facing context created by the factory
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
        var workflowDefinitionId = VersionedType.of(
                new QualifiedName(configuredName),
                workflowConfiguration.workflowVersion()
        );

        this.contextDelegate = new WorkflowExecutionOperationsDelegation(
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

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The body runs inside a unit of work spanning the instance's entire lifetime. It is created from the
     * non-transactional {@link WorkflowExecutionOperationsDelegation#workflowBodyUnitOfWorkFactory()} so that no
     * transactional resources are held while the instance is parked.
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

                            try {
                                publishStartWorkflow(ctx);
                                executeWorkflow(ctx);
                            } catch (FutureResolutionTimeoutException timeout) {
                                logPublicationResolutionTimeout(timeout);
                                throw timeout;
                            } catch (Throwable e) {
                                try {
                                    handleWorkflowException(ctx, e);
                                } catch (FutureResolutionTimeoutException timeout) {
                                    logPublicationResolutionTimeout(timeout);
                                    throw timeout;
                                }
                            } finally {
                                // A non-terminal exit keeps the instance for recovery, unless another writer owns it.
                                if (this.state().workflowStatus().isTerminal()
                                        || stopReason.get() == StopReason.APPEND_REJECTED) {
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
    private CompletableFuture<WorkflowExecutionOperationsDelegation> skipTerminalInstance() {
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
            publishAndWait(startedWorkflow(this.workflowExecutionOperations(), workflowName,
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
        var workflowPayload = this.workflowExecutionOperations().workflowPayload();
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
                publishAndWait(completedWorkflow(this.workflowExecutionOperations(), workflowName,
                                                 workflowState.workflowDefinitionId(), eventNameCustomizer), ctx);
            });
        }
        logger.info("Workflow executed. Resulting workflow payload {}.",
                    this.workflowExecutionOperations().workflowPayload());
    }

    /**
     * Handles exceptions thrown during workflow execution.
     * <p>
     * This method applies the workflow exception policy and may publish terminal events. The workflow ends on an
     * explicit {@link WorkflowFailedException}, a {@link StepFailedException} the body did not handle, a cancellation,
     * a workflow timeout, or any other exception the configuration does not classify as recoverable through
     * {@link WorkflowConfiguration#recoverableExceptionPolicy()}. A recoverable exception, a replay drift and an engine
     * shutdown leave the workflow in its current non-terminal state, so the workflow driver stops runtime execution and
     * the instance is re-driven from durable history on the next start. A {@link FutureResolutionTimeoutException}
     * raised by one of the publication attempts propagates to the workflow driver the same way.
     *
     * @param ctx       processing context.
     * @param exception exception to handle.
     */
    private void handleWorkflowException(ProcessingContext ctx, Throwable exception) {
        if (completeCancellationRequest(ctx)) {
            return;
        }
        var names = this.workflowConfiguration.eventNameCustomizer();
        var definitionId = workflowState.workflowDefinitionId();
        switch (exception) {
            case Throwable rejected when isRejected(rejected) -> logger.debug(
                    "Workflow {} stopped after an append rejection because another writer already "
                            + "recorded the event this execution tried to append.",
                    workflowId);
            // Whatever the exception type, a store failure is not a defect in the body.
            case Throwable storeFailure when stopReason.get() == StopReason.APPEND_FAILED -> logPaused(
                    "the event store did not accept one of its events",
                    storeFailure);
            // If WorkflowLifecycleControlDelegate already published the terminal event, endWith does nothing.
            case WorkflowFailedException wfe -> endWith(ctx,
                                                        () -> failedWorkflow(workflowExecutionOperations(),
                                                                             workflowName,
                                                                             wfe,
                                                                             definitionId,
                                                                             names));
            case WorkflowCancelledException wce -> endWith(ctx,
                                                           () -> cancelledWorkflow(workflowExecutionOperations(),
                                                                                   workflowName,
                                                                                   wce,
                                                                                   definitionId,
                                                                                   names));
            case TimeoutException te -> endWith(ctx, () -> timeoutWorkflow(workflowExecutionOperations(), workflowName,
                                                                           contextDelegate.clock().instant(),
                                                                           definitionId, names));
            // No terminal event: the next replay runs cleanly once the code is reverted or wrapped in
            // ctx.migrateVersion(...).
            case WorkflowReplayDriftException drift -> logger.warn("Workflow {} paused due to replay drift: {}. "
                                                                           + "Revert the code change or wrap it in ctx.migrateVersion() and replay.",
                                                                   workflowId, drift.getMessage());
            case InterruptedException ie -> Thread.currentThread().interrupt();
            // Engine shutdown, not a step outcome: the workflow resumes on the next start.
            case StepInterruptedException sie -> logger.debug("Workflow {} driver stopped: {}",
                                                              workflowId,
                                                              sie.getMessage());
            case StepFailedException sfe -> endWith(ctx,
                                                    () -> failedWorkflow(workflowExecutionOperations(),
                                                                         workflowName,
                                                                         sfe,
                                                                         definitionId,
                                                                         names));
            case Throwable recoverable
                    when workflowConfiguration.recoverableExceptionPolicy().isRecoverable(recoverable) -> logPaused(
                    "a recoverable exception in its body",
                    recoverable);
            default -> {
                logger.error("Workflow {} failed after an unhandled exception in its body.", workflowId, exception);
                var failure = new WorkflowFailedException("Unhandled exception in workflow body", exception);
                endWith(ctx,
                        () -> failedWorkflow(workflowExecutionOperations(),
                                             workflowName,
                                             failure,
                                             definitionId,
                                             names));
            }
        }
    }

    /**
     * Publishes the terminal event, unless the workflow already reached a terminal status.
     */
    private void endWith(ProcessingContext ctx, Supplier<EventMessage> terminalEvent) {
        if (!this.state().workflowStatus().isTerminal()) {
            terminalTransition.transition(() -> publishAndWait(terminalEvent.get(), ctx));
        }
    }

    private void logPaused(String reason, Throwable cause) {
        logger.error("Workflow {} paused after {}. It stays non-terminal and is re-driven on the next start of the "
                             + "processing node or claim of its segment.", workflowId, reason, cause);
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
                publishAndWait(cancelledWorkflow(this.workflowExecutionOperations(), workflowName, cancellation.cause(),
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
     * Stops the in-memory workflow driver after the body exited without a terminal status and without an append
     * rejection: a publication that could not be resolved, an engine shutdown, a replay drift pause, or an unhandled
     * failure.
     * <p>
     * The execution deliberately remains in the repository and non-terminal so the next processing-node start or
     * segment claim can restore it from durable history. In contrast to {@link #finishWorkflow(Consumer)}, this method
     * must not invoke the termination handler because that handler removes the execution from the engine.
     */
    private void stopRuntimeForRecovery() {
        this.stoppedForRecovery = true;
        stopRuntime(new StepInterruptedException("Workflow runtime stopped for recovery"));
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
        // An event another instance published through the publish primitive is broadcast to every owned execution.
        // It carries that instance's step metadata, so it may wake a wait here but never evolves this state.
        boolean ownState = !isForeignStep(eventMessage);
        if (running) {
            // live mode
            appendTask(i -> {
                eventWaitConditions.evaluateAndApply(eventMessage, processingContext, contextDelegate::eventReceived);
                if (ownState) {
                    workflowState.evolve(eventMessage, processingContext);
                }
            });
        } else if (stoppedForRecovery) {
            // Keep the projected state in sync with durable events, but queue no work: no driver runs until restart.
            if (ownState) {
                workflowState.evolve(eventMessage, processingContext, false);
            }
        } else {
            // replay mode
            if (ownState) {
                workflowState.evolve(eventMessage, processingContext, false);
            }
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

    private boolean isForeignStep(EventMessage eventMessage) {
        var metadata = eventMessage.metadata();
        return MetadataUtils.hasWorkflowId().test(metadata)
                && !workflowId.equals(MetadataUtils.getWorkflowId(metadata));
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public CompletableFuture<Void> appendWorkflowEvent(EventMessage eventMessage, Context parentContext) {
        return appender.appendSequentially(marker -> workflowEventPublisher.publish(
                eventMessage,
                parentContext,
                appendConditionFor(marker)
        )).whenComplete((ignored, failure) -> {
            if (failure == null) {
                return;
            }
            if (isRejected(failure)) {
                logger.warn("Append of {} for workflow '{}' was rejected: another writer already recorded events for "
                                    + "this instance. Stopping this execution.",
                            eventMessage.type(), workflowId);
                // A rejection wins over an earlier store failure: the execution must leave this engine.
                stopReason.set(StopReason.APPEND_REJECTED);
                interruptWorkflowDriver();
            } else {
                stopReason.compareAndSet(null, StopReason.APPEND_FAILED);
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
     * @param event   event to append
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
    public WorkflowExecutionOperations workflowExecutionOperations() {
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
        appender.updateMarker(position);
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

    /**
     * Why one of this execution's own appends stopped it. A rejection means another writer recorded an event for the
     * instance first, so this execution leaves the engine. Any other failure means the store did not accept the event,
     * so the workflow pauses and is re-driven from its history.
     */
    private enum StopReason {
        APPEND_REJECTED,
        APPEND_FAILED
    }
}
