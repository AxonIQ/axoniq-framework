/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepWaitInfo;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.lock.LockFactory;
import org.axonframework.common.lock.PessimisticLockFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Hibernates the idle workflows of a {@link WorkflowEngine}: a workflow that only waits for an external event or a
 * deadline leaves memory, and is restored from the event store when that event arrives or that deadline expires.
 * <p>
 * Residency follows the execution repository, the way an Axon Framework saga repository owns which sagas are in
 * memory: a workflow is in memory while the {@link WorkflowExecutionRepository} holds its execution. A lock per
 * workflow id, from a {@link PessimisticLockFactory}, keeps hibernation, event delivery, cancellation and wakes of one
 * workflow apart, so an event or a cancellation either reaches the execution in memory or finds the workflow
 * hibernated, never neither. The execution itself only reports whether it is at a safe point, through its
 * {@link WorkflowExecutionHibernationSupport}.
 * <p>
 * Every 10 seconds, a pass asks the running executions in memory to hibernate. A hibernated workflow keeps a timer for
 * its nearest deadline and a stand-in {@link WorkflowCancellation}, and its wait association rows stay in the index.
 * <p>
 * The {@link WorkflowEngine} creates it when the hibernation setting of the workflow module is enabled, starts it with
 * the engine and stops it on shutdown. Internal, because the policy, the interval and the bookkeeping are details of
 * the engine.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
final class WorkflowEngineHibernationSupport {

    /**
     * Time between 2 passes. Hibernation is meant for workflows that wait for days, so a workflow that stays in memory
     * for up to this long after it started to wait costs nothing that matters.
     */
    static final Duration PASS_INTERVAL = Duration.ofSeconds(10);
    /**
     * Time before a hibernated workflow whose wake failed, for example because the event store was unreachable, is
     * woken again. Its deadline timer already fired, and no event matches a sleep, so nothing else would wake it.
     */
    static final Duration DEFAULT_WAKE_RETRY_DELAY = Duration.ofSeconds(10);
    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineHibernationSupport.class);

    private final WorkflowScheduler scheduler;
    private final Clock clock;
    private final WorkflowExecutionRepository repository;
    private final WorkflowCancellationService cancellationService;
    private final UnsafeCheckpointWorkIndex checkpointWorkIndex;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Supplier<Duration> restoreTimeout;
    private final WorkflowLoader loader;
    private final LockFactory locks = PessimisticLockFactory.usingDefaults();
    private final Map<String, HibernatedWorkflow> hibernated = new ConcurrentHashMap<>();
    // Wakes for a deadline or a cancellation load from the event store, so they never run on the timer thread.
    private final Executor wakeExecutor = command -> Thread.ofVirtual().name("workflow-wake").start(command);
    // Cleared on stop, so no workflow hibernates after the hibernated ones are dropped.
    private volatile boolean running;
    private volatile WorkflowScheduler.@Nullable ScheduledTask nextPass;
    // Package-private so a test can shrink it instead of waiting out the production delay.
    Duration wakeRetryDelay = DEFAULT_WAKE_RETRY_DELAY;

    /**
     * Creates the hibernation support of an engine.
     *
     * @param scheduler           schedules the passes and the deadlines of hibernated workflows
     * @param clock               the clock the scheduler measures deadlines with
     * @param repository          the executions in memory
     * @param cancellationService the cancellation registrations of the live workflows
     * @param checkpointWorkIndex the workflows with unsafe checkpoint work
     * @param unitOfWorkFactory   creates the unit of work of a wake outside event handling
     * @param restoreTimeout      the time a wake may take to load a workflow
     * @param loader              loads a hibernated workflow back into the engine and starts it
     */
    WorkflowEngineHibernationSupport(WorkflowScheduler scheduler,
                                     Clock clock,
                                     WorkflowExecutionRepository repository,
                                     WorkflowCancellationService cancellationService,
                                     UnsafeCheckpointWorkIndex checkpointWorkIndex,
                                     UnitOfWorkFactory unitOfWorkFactory,
                                     Supplier<Duration> restoreTimeout,
                                     WorkflowLoader loader) {
        this.scheduler = Objects.requireNonNull(scheduler, "The WorkflowScheduler must not be null.");
        this.clock = Objects.requireNonNull(clock, "The Clock must not be null.");
        this.repository = Objects.requireNonNull(repository, "The WorkflowExecutionRepository must not be null.");
        this.cancellationService = Objects.requireNonNull(cancellationService,
                                                          "The WorkflowCancellationService must not be null.");
        this.checkpointWorkIndex = Objects.requireNonNull(checkpointWorkIndex,
                                                          "The UnsafeCheckpointWorkIndex must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory must not be null.");
        this.restoreTimeout = Objects.requireNonNull(restoreTimeout, "The restore timeout must not be null.");
        this.loader = Objects.requireNonNull(loader, "The WorkflowLoader must not be null.");
    }

    /**
     * Starts the passes.
     */
    void start() {
        running = true;
        scheduleNextPass();
    }

    /**
     * Stops the passes and forgets every hibernated workflow. A pass that is running finishes.
     */
    void stop() {
        running = false;
        var pass = nextPass;
        if (pass != null) {
            pass.cancel();
        }
        drop(workflowId -> true);
    }

    /**
     * Asks every running execution in memory to hibernate at its next safe point.
     */
    void pass() {
        for (var execution : repository.findAll()) {
            try {
                if (execution instanceof WorkflowHibernationProvider provider) {
                    provider.workflowHibernation().request();
                }
            } catch (RuntimeException e) {
                logger.warn("Could not ask workflow [{}] to hibernate.", execution.workflowId(), e);
            }
        }
    }

    /**
     * Enables hibernation for the given execution, which the engine just added to the repository.
     *
     * @param execution the execution that may hibernate from now on
     */
    void allow(WorkflowExecution execution) {
        if (execution instanceof WorkflowHibernationProvider provider) {
            provider.workflowHibernation().enable(this::hibernate);
        }
    }

    /**
     * Delivers the event to the given workflow: to its execution in memory, or, when it is hibernated, to the workflow
     * restored from the event store.
     *
     * @param workflowId the identifier of the workflow the event is for
     * @param event      the event to deliver
     * @param context    the context of the event, which a restored execution runs on
     * @return {@code false} when the workflow is neither in memory nor hibernated here, so it belongs to another
     * engine, which is expected in multi-module setups
     */
    boolean deliver(String workflowId, EventMessage event, ProcessingContext context) {
        try (var ignored = locks.obtainLock(workflowId)) {
            var execution = repository.findById(workflowId);
            if (execution.isPresent()) {
                execution.get().onEvent(event, context);
                return true;
            }
            var hibernatedWorkflow = hibernated.get(workflowId);
            if (hibernatedWorkflow == null) {
                return false;
            }
            // ponytail: the processor thread waits for the load; load asynchronously if wakes show up in profiles
            wake(workflowId, hibernatedWorkflow, context, restored -> restored.onEvent(event, context));
            return true;
        }
    }

    /**
     * Whether the given workflow is hibernated here, so its waits stay in the {@link WaitAssociationIndex}.
     *
     * @param workflowId the identifier of the workflow
     * @return {@code true} when the workflow is hibernated here
     */
    boolean isHibernated(String workflowId) {
        return hibernated.containsKey(workflowId);
    }

    /**
     * Whether a start for the given workflow id is a duplicate of a live workflow, hibernated or in memory. The
     * execution is read once more after the hibernated workflows: a wake adds the execution before it forgets the
     * hibernated workflow, so a workflow woken meanwhile is still seen.
     *
     * @param workflowId the identifier the start would create
     * @return {@code true} when the start is a duplicate and must be ignored
     */
    boolean isDuplicateStart(String workflowId) {
        if (!hibernated.containsKey(workflowId) && repository.findById(workflowId).isEmpty()) {
            return false;
        }
        logger.warn("A workflow with id '{}' is already running; ignoring the new start request. Workflow ids MUST be "
                            + "unique, so the start is treated as a duplicate.", workflowId);
        return true;
    }

    /**
     * Decorates the cancellation of the given execution, so a request runs against the execution only while it is in
     * memory. A request that arrives after the execution hibernated goes to the registration that replaced it: the
     * stand-in of the hibernated workflow, or the cancellation of the restored execution.
     *
     * @param execution    the execution the cancellation belongs to
     * @param cancellation the cancellation of the execution
     * @return the cancellation to register for the workflow
     */
    WorkflowCancellation decorate(WorkflowExecution execution, WorkflowCancellation cancellation) {
        return new HibernationAwareWorkflowCancellation(execution, cancellation);
    }

    /**
     * Decorates a checkpoint listener of the given execution, so only the execution in memory reports its checkpoint
     * work. An execution that hibernated keeps unwinding for a moment, and must not mark the workflow safe or unsafe
     * on behalf of the restored one.
     *
     * @param execution the execution the listener belongs to
     * @param listener  the listener of the checkpoint work index
     * @return the listener to register on the execution
     */
    CheckpointWorkStateListener decorate(WorkflowExecution execution, CheckpointWorkStateListener listener) {
        return new CheckpointWorkStateListener() {
            @Override
            public void onMarkedUnsafe() {
                if (isInMemory(execution)) {
                    listener.onMarkedUnsafe();
                }
            }

            @Override
            public void onMarkedSafe() {
                if (isInMemory(execution)) {
                    listener.onMarkedSafe();
                }
            }
        };
    }

    /**
     * Forgets the hibernated workflows matching the given filter and cancels their deadlines. Their durable state is
     * untouched: the next claim of their segment restores them.
     * <p>
     * Takes the lock of each workflow, so a wake that is loading finishes first. Its execution is then in memory, and
     * the engine stops it with the other executions.
     *
     * @param filter selects the workflows to forget by identifier
     */
    void drop(Predicate<String> filter) {
        hibernated.forEach((workflowId, hibernatedWorkflow) -> {
            if (!filter.test(workflowId)) {
                return;
            }
            try (var ignored = locks.obtainLock(workflowId)) {
                if (hibernated.remove(workflowId, hibernatedWorkflow)) {
                    hibernatedWorkflow.deadline().cancel();
                    cancellationService.unregister(workflowId);
                }
            }
        });
    }

    private boolean isInMemory(WorkflowExecution execution) {
        return repository.findById(execution.workflowId()).orElse(null) == execution;
    }

    /**
     * Records the given execution as hibernated and removes it from memory, when it is at a safe point. Runs on the
     * workflow thread, under the lock of the workflow, so no event or cancellation can reach the execution between the
     * safe point check and its removal.
     *
     * @return {@code false}, keeping the workflow in memory, when it is not at a safe point, or when one of its waits
     * has no readable timeout, because nothing could wake that wait again
     */
    private boolean hibernate(WorkflowExecution execution, Supplier<Optional<Set<String>>> safePoint) {
        var workflowId = execution.workflowId();
        try (var ignored = locks.obtainLock(workflowId)) {
            if (!running || !isInMemory(execution)) {
                return false;
            }
            var openWaits = safePoint.get();
            if (openWaits.isEmpty()) {
                return false;
            }
            var deadline = nearestDeadline(execution, openWaits.get());
            if (deadline.isEmpty()) {
                logger.debug("Not hibernating workflow [{}]: not each of its waits {} has a readable timeout.",
                             workflowId, openWaits.get());
                return false;
            }
            var segment = Segment.fromContext(execution.processingContext()).orElse(null);
            var timer = scheduler.schedule(deadline.get());
            hibernated.put(workflowId, new HibernatedWorkflow(segment, timer));
            cancellationService.register(workflowId, new HibernatedWorkflowCancellation(workflowId));
            repository.remove(workflowId);
            checkpointWorkIndex.markSafe(workflowId);
            timer.completion().thenRunAsync(() -> wakeDetached(workflowId, restored -> {
            }), wakeExecutor);
            logger.debug("Workflow [{}] hibernated until an awaited event or its deadline at {}.",
                         workflowId, deadline.get());
            return true;
        }
    }

    /**
     * Returns the nearest timeout of the given open waits. Each timeout is read the way {@link WaitForDelegate} reads
     * it: the result of the wait's STARTED step, converted to the {@link StepWaitInfo} the wait published.
     *
     * @return the nearest timeout, or empty when one of the waits has no readable timeout
     */
    private static Optional<Instant> nearestDeadline(WorkflowExecution execution, Set<String> openWaits) {
        var converter = execution.processingContext().component(EventConverter.class);
        var state = execution.state();
        Instant nearest = null;
        for (var stepName : openWaits) {
            var step = state.getStep(stepName);
            var info = step == null || converter == null ? null : converter.convert(step.result(), StepWaitInfo.class);
            if (info == null || info.timeoutTime() == null) {
                return Optional.empty();
            }
            if (nearest == null || info.timeoutTime().isBefore(nearest)) {
                nearest = info.timeoutTime();
            }
        }
        return Optional.ofNullable(nearest);
    }

    /**
     * Restores a hibernated workflow from the event store and starts it. The caller holds the lock of the workflow.
     * <p>
     * The given action runs on the restored execution before its body starts, so the body cannot hibernate again
     * before it sees what woke it.
     */
    private void wake(String workflowId,
                      HibernatedWorkflow hibernatedWorkflow,
                      ProcessingContext executionContext,
                      Consumer<WorkflowExecution> beforeStart) {
        loader.load(workflowId, executionContext, hibernatedWorkflow.segment(), beforeStart);
        hibernated.remove(workflowId, hibernatedWorkflow);
        hibernatedWorkflow.deadline().cancel();
        logger.debug("Workflow [{}] woken from hibernation.", workflowId);
    }

    /**
     * Wakes a hibernated workflow outside event handling, for its deadline or a cancellation request. The restored
     * execution runs on a unit of work of its own, the way a claimed segment's executions do.
     */
    private void wakeDetached(String workflowId, Consumer<WorkflowExecution> beforeStart) {
        var hibernatedWorkflow = hibernated.get(workflowId);
        if (hibernatedWorkflow == null) {
            return;
        }
        try {
            unitOfWorkFactory.create("wake-" + workflowId)
                             .executeWithResult(executionContext -> {
                                 var segment = hibernatedWorkflow.segment();
                                 if (segment != null) {
                                     executionContext.putResource(Segment.RESOURCE_KEY, segment);
                                 }
                                 try (var ignored = locks.obtainLock(workflowId)) {
                                     var current = hibernated.get(workflowId);
                                     if (current != null) {
                                         wake(workflowId, current, executionContext, beforeStart);
                                     }
                                 }
                                 return CompletableFuture.completedFuture(null);
                             })
                             .orTimeout(restoreTimeout.get().toMillis(), TimeUnit.MILLISECONDS)
                             .join();
        } catch (RuntimeException e) {
            logger.warn("Could not wake hibernated workflow [{}]. Trying again in {}.", workflowId, wakeRetryDelay, e);
            retryWake(workflowId);
        }
    }

    /**
     * Replaces the timer of a hibernated workflow whose wake failed with one that wakes it again after
     * {@link #wakeRetryDelay}, unless it was woken or dropped meanwhile.
     */
    private void retryWake(String workflowId) {
        if (!running) {
            return;
        }
        try (var ignored = locks.obtainLock(workflowId)) {
            var current = hibernated.get(workflowId);
            if (current == null) {
                return;
            }
            var retry = scheduler.schedule(clock.instant().plus(wakeRetryDelay));
            current.deadline().cancel();
            hibernated.put(workflowId, new HibernatedWorkflow(current.segment(), retry));
            retry.completion().thenRunAsync(() -> wakeDetached(workflowId, restored -> {
            }), wakeExecutor);
        }
    }

    private void scheduleNextPass() {
        if (!running) {
            return;
        }
        var pass = scheduler.schedule(clock.instant().plus(PASS_INTERVAL));
        nextPass = pass;
        pass.completion().thenRun(() -> {
            try {
                pass();
            } finally {
                scheduleNextPass();
            }
        });
    }

    /**
     * Loads a hibernated workflow back into the engine and starts it, on behalf of this support, the way a
     * {@link org.axonframework.modelling.repository.SimpleRepositoryEntityLoader} loads an entity for a repository.
     */
    @FunctionalInterface
    interface WorkflowLoader {

        /**
         * Loads the workflow from the event store into memory and starts it, after handing the loaded execution to the
         * given action. A workflow that another node ended while it hibernated is forgotten instead.
         *
         * @param workflowId       the identifier of the hibernated workflow
         * @param executionContext the context the restored execution runs on
         * @param segment          the segment that owns the workflow, or {@code null} outside a segmented processor
         * @param beforeStart      receives the loaded execution before its body starts
         */
        void load(String workflowId,
                  ProcessingContext executionContext,
                  @Nullable Segment segment,
                  Consumer<WorkflowExecution> beforeStart);
    }

    /**
     * A workflow that left memory at a safe point.
     *
     * @param segment  the segment that owns it, or {@code null} outside a segmented processor
     * @param deadline the timer that wakes it at its nearest deadline
     */
    private record HibernatedWorkflow(@Nullable Segment segment, WorkflowScheduler.ScheduledTask deadline) {

    }

    /**
     * Runs the cancellation requests of one execution while it is in memory. Once the execution hibernated, a request
     * goes to the registration that replaced this one. A workflow that left the engine otherwise, because it ended or
     * its segment was released, keeps the behavior of its own cancellation.
     */
    private final class HibernationAwareWorkflowCancellation implements WorkflowCancellation.Request {

        private final WorkflowExecution execution;
        private final WorkflowCancellation delegate;

        private HibernationAwareWorkflowCancellation(WorkflowExecution execution, WorkflowCancellation delegate) {
            this.execution = execution;
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause) {
            return whileInMemory(
                    cancellation -> cancellation.requestStepCancellation(stepName, cause),
                    () -> cancellationService.requestStepCancellation(execution.workflowId(), stepName, cause)
            );
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return whileInMemory(
                    cancellation -> cancellation.requestCancellationOfAllSteps(cause),
                    () -> cancellationService.requestCancellationOfAllSteps(execution.workflowId(), cause)
            );
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return whileInMemory(
                    cancellation -> cancellation.requestWorkflowCancellation(cause),
                    () -> cancellationService.requestWorkflowCancellation(execution.workflowId(), cause)
            );
        }

        @Override
        public boolean hasPendingWorkflowCancellation() {
            return delegate instanceof WorkflowCancellation.Request request
                    && request.hasPendingWorkflowCancellation();
        }

        @Override
        public WorkflowCancellation.@Nullable PendingRequest consumeWorkflowCancellation() {
            return delegate instanceof WorkflowCancellation.Request request
                    ? request.consumeWorkflowCancellation()
                    : null;
        }

        @Override
        public void abortPendingWorkflowCancellation(CancellationException reason) {
            if (delegate instanceof WorkflowCancellation.Request request) {
                request.abortPendingWorkflowCancellation(reason);
            }
        }

        private <T> CompletableFuture<T> whileInMemory(Function<WorkflowCancellation, CompletableFuture<T>> request,
                                                       Supplier<CompletableFuture<T>> afterHibernation) {
            var workflowId = execution.workflowId();
            try (var ignored = locks.obtainLock(workflowId)) {
                var inMemory = repository.findById(workflowId).orElse(null);
                if (inMemory == execution || (inMemory == null && !hibernated.containsKey(workflowId))) {
                    return request.apply(delegate);
                }
            }
            return afterHibernation.get();
        }
    }

    /**
     * Stands in for the cancellation of a hibernated workflow. It wakes the workflow and queues the request as a task
     * of the restored execution before its body starts, the way an event that wakes a workflow is queued.
     * <p>
     * A restored body takes no task before it waits again where it hibernated, because every earlier wait is answered
     * from its recorded state. So the request runs once the body replayed its steps and they are registered again.
     */
    private final class HibernatedWorkflowCancellation implements WorkflowCancellation {

        private final String workflowId;

        private HibernatedWorkflowCancellation(String workflowId) {
            this.workflowId = workflowId;
        }

        @Override
        public CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause) {
            return wakeThen(() -> cancellationService.requestStepCancellation(workflowId, stepName, cause));
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return wakeThen(() -> cancellationService.requestCancellationOfAllSteps(workflowId, cause));
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return wakeThen(() -> cancellationService.requestWorkflowCancellation(workflowId, cause));
        }

        private <T> CompletableFuture<T> wakeThen(Supplier<CompletableFuture<T>> request) {
            var result = new CompletableFuture<T>();
            var queued = new AtomicBoolean();
            CompletableFuture
                    .runAsync(() -> wakeDetached(workflowId, restored -> {
                        queued.set(true);
                        restored.appendTask(ignored -> completeWith(result, request));
                    }), wakeExecutor)
                    .whenComplete((ignored, failure) -> {
                        if (queued.get()) {
                            return;
                        }
                        if (failure != null) {
                            result.completeExceptionally(failure);
                        } else if (hibernated.containsKey(workflowId)) {
                            result.completeExceptionally(new IllegalStateException(
                                    "Workflow '" + workflowId + "' could not be woken from hibernation"
                            ));
                        } else {
                            // Another wake restored it first, so queue the request there too, or it ended elsewhere
                            // and the cancellation service answers for it.
                            repository.findById(workflowId).ifPresentOrElse(
                                    restored -> restored.appendTask(task -> completeWith(result, request)),
                                    () -> completeWith(result, request)
                            );
                        }
                    });
            return result;
        }

        private static <T> void completeWith(CompletableFuture<T> result, Supplier<CompletableFuture<T>> request) {
            try {
                request.get().whenComplete((value, failure) -> {
                    if (failure != null) {
                        result.completeExceptionally(failure);
                    } else {
                        result.complete(value);
                    }
                });
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }
    }
}
