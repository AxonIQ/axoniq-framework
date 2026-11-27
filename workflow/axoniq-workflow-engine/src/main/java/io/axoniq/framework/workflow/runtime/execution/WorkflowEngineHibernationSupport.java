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
import java.util.HashMap;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
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
 * Every hibernation interval, a pass asks the running executions in memory to hibernate. A hibernated workflow keeps a
 * timer for its nearest deadline and a stand-in {@link WorkflowCancellation}, and its wait association rows stay in the
 * index.
 * <p>
 * With a {@link WorkflowIndex} that keeps wake times, the next claim of a segment, on any node, adopts the hibernated
 * workflows whose open waits did not change, instead of loading them into memory. An adopted workflow wakes only once
 * the claim started its restored executions, so a wake never starts a body twice and never runs one while the segment
 * catches up. Without such an index, a claim restores every running workflow of the segment.
 * <p>
 * The {@link WorkflowEngine} creates it when the hibernation setting of the workflow module is enabled, starts it with
 * the engine and stops it on shutdown.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WorkflowEngineHibernationSupport {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineHibernationSupport.class);

    private final WorkflowScheduler scheduler;
    private final Clock clock;
    private final WorkflowExecutionRepository repository;
    private final WorkflowCancellationService cancellationService;
    private final UnsafeCheckpointWorkIndex checkpointWorkIndex;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Supplier<Duration> restoreTimeout;
    private final WorkflowLoader loader;
    private final Duration interval;
    private final Executor wakeExecutor;
    private final Supplier<@Nullable WorkflowEngineIndexSupport> indexSupport;
    private final LockFactory locks = PessimisticLockFactory.usingDefaults();
    private final Map<String, Hibernation> hibernations = new ConcurrentHashMap<>();
    // The hibernated workflows of each segment by segment id, so a segment's are found directly.
    private final Map<Integer, Set<String>> hibernatedBySegment = new ConcurrentHashMap<>();
    // The claim in progress of each segment and the claimed segments, by segment id.
    private final Map<Integer, SegmentClaim> claims = new ConcurrentHashMap<>();
    private final Map<Integer, Segment> claimedSegments = new ConcurrentHashMap<>();
    // Keeps a hibernation and the completion of a claim apart from a release of the same segment.
    private final ReentrantLock segmentLock = new ReentrantLock();
    // Cleared on stop, so no workflow hibernates after the hibernated ones are dropped.
    private volatile boolean running;
    private volatile WorkflowScheduler.@Nullable ScheduledTask nextPass;

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
     * @param interval            the time between 2 passes, and before a failed wake is tried again
     * @param wakeExecutor        runs the wakes for a deadline or a cancellation, which load from the store, so they
     *                            never run on the timer thread
     * @param indexSupport        supplies the index support of the engine, or {@code null} without a
     *                            {@link WorkflowIndex}
     */
    WorkflowEngineHibernationSupport(WorkflowScheduler scheduler,
                                     Clock clock,
                                     WorkflowExecutionRepository repository,
                                     WorkflowCancellationService cancellationService,
                                     UnsafeCheckpointWorkIndex checkpointWorkIndex,
                                     UnitOfWorkFactory unitOfWorkFactory,
                                     Supplier<Duration> restoreTimeout,
                                     WorkflowLoader loader,
                                     Duration interval,
                                     Executor wakeExecutor,
                                     Supplier<@Nullable WorkflowEngineIndexSupport> indexSupport) {
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
        this.interval = Objects.requireNonNull(interval, "The interval must not be null.");
        this.wakeExecutor = Objects.requireNonNull(wakeExecutor, "The wake Executor must not be null.");
        this.indexSupport = Objects.requireNonNull(indexSupport, "The index support supplier must not be null.");
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
        drop(Map.copyOf(hibernations));
        claims.clear();
        claimedSegments.clear();
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
            var hibernation = hibernations.get(workflowId);
            if (hibernation == null) {
                return false;
            }
            // The processor thread waits until the workflow is loaded.
            wake(workflowId, hibernation, context, restored -> restored.onEvent(event, context));
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
        return hibernations.containsKey(workflowId);
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
        return hibernations.containsKey(workflowId) || repository.findById(workflowId).isPresent();
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
     * Forgets the given hibernated workflows and cancels their deadlines. Their durable state is untouched: the next
     * claim of their segment restores them.
     * <p>
     * Takes the lock of each workflow, so a wake that is loading finishes first. Its execution is then in memory, and
     * the engine stops it with the other executions.
     */
    private void drop(Map<String, Hibernation> dropped) {
        dropped.forEach((workflowId, hibernation) -> {
            try (var ignored = locks.obtainLock(workflowId)) {
                if (removeHibernation(workflowId, hibernation)) {
                    hibernation.wakeTimer().cancel();
                    cancellationService.unregister(workflowId);
                    // A wake held until its segment started finds the workflow gone and does nothing.
                    hibernation.armed().complete(null);
                }
            }
        });
    }

    /**
     * Adds the hibernation of the given workflow, also to the hibernated workflows of its segment. The caller holds
     * the lock of the workflow.
     */
    private void addHibernation(String workflowId, Hibernation hibernation) {
        hibernations.put(workflowId, hibernation);
        var segment = hibernation.segment();
        if (segment != null) {
            hibernatedBySegment.computeIfAbsent(segment.getSegmentId(), segmentId -> ConcurrentHashMap.newKeySet())
                               .add(workflowId);
        }
    }

    /**
     * Removes the given hibernation of the given workflow, unless another one replaced it. The caller holds the lock
     * of the workflow.
     *
     * @return {@code true} when the given hibernation was removed
     */
    private boolean removeHibernation(String workflowId, Hibernation hibernation) {
        if (!hibernations.remove(workflowId, hibernation)) {
            return false;
        }
        var segment = hibernation.segment();
        var ofSegment = segment == null ? null : hibernatedBySegment.get(segment.getSegmentId());
        if (ofSegment != null) {
            ofSegment.remove(workflowId);
        }
        return true;
    }

    /**
     * Returns the hibernations of the workflows that hibernated in, or were adopted by, the given segment.
     */
    private Map<String, Hibernation> hibernationsOf(Segment segment) {
        var ofSegment = new HashMap<String, Hibernation>();
        for (var workflowId : hibernatedBySegment.getOrDefault(segment.getSegmentId(), Set.of())) {
            var hibernation = hibernations.get(workflowId);
            if (hibernation != null && hibernation.segment() != null
                    && hibernation.segment().getSegmentId() == segment.getSegmentId()) {
                ofSegment.put(workflowId, hibernation);
            }
        }
        return ofSegment;
    }

    /**
     * Starts a claim of the given segment. Only this claim may adopt the segment's hibernated workflows, until it is
     * {@link #claimed(Segment, SegmentClaim) completed} or the segment is released.
     * <p>
     * It first forgets the hibernated workflows the segment owns that are still listed under an earlier segment. A
     * split or a merge replaces segments without releasing them, and the claim restores or adopts those workflows
     * anew.
     *
     * @param segment the segment being claimed
     * @return the claim, which the claim passes to
     * {@link #adopt(String, Segment, HibernatedWorkflows.HibernatedWorkflow, SegmentClaim)} and
     * {@link #claimed(Segment, SegmentClaim)}
     */
    SegmentClaim claiming(Segment segment) {
        segmentLock.lock();
        try {
            // A split or a merge replaced the claimed segments this one overlaps, so none of them owns a workflow now.
            claimedSegments.values().removeIf(claimed -> claimed.matches(segment.getSegmentId())
                    || segment.matches(claimed.getSegmentId()));
        } finally {
            segmentLock.unlock();
        }
        var leftovers = new HashMap<String, Hibernation>();
        hibernations.forEach((workflowId, hibernation) -> {
            if (WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                leftovers.put(workflowId, hibernation);
            }
        });
        drop(leftovers);
        var claim = new SegmentClaim();
        claims.put(segment.getSegmentId(), claim);
        return claim;
    }

    /**
     * Completes the given claim, so the segment's workflows may hibernate from now on. Does nothing when the segment
     * was released meanwhile, because a claim that timed out keeps loading after its release.
     *
     * @param segment the claimed segment
     * @param claim   the claim {@link #claiming(Segment)} returned
     */
    void claimed(Segment segment, SegmentClaim claim) {
        segmentLock.lock();
        try {
            if (claims.remove(segment.getSegmentId(), claim)) {
                claimedSegments.put(segment.getSegmentId(), segment);
            }
        } finally {
            segmentLock.unlock();
        }
    }

    /**
     * Keeps the given workflow of a claimed segment hibernated instead of restoring it. It gets a deadline
     * timer and a stand-in cancellation as when it hibernated, but wakes only after {@link #arm(Segment)} for its
     * segment.
     *
     * @param workflowId the identifier of the workflow
     * @param segment    the claimed segment that owns it
     * @param workflow   the hibernated workflow, whose open waits the caller found unchanged
     * @param claim      the claim {@link #claiming(Segment)} returned
     * @return {@code false}, so the claim restores the workflow, when hibernation stopped, the segment was released
     * since the claim started, or the workflow is already known here
     */
    boolean adopt(String workflowId,
                  Segment segment,
                  HibernatedWorkflows.HibernatedWorkflow workflow,
                  SegmentClaim claim) {
        try (var ignored = locks.obtainLock(workflowId)) {
            if (!running
                    || claims.get(segment.getSegmentId()) != claim
                    || hibernations.containsKey(workflowId)
                    || repository.findById(workflowId).isPresent()) {
                return false;
            }
            park(workflowId, segment, workflow.openWaits(), workflow.wakesAt(), new CompletableFuture<>());
            return true;
        }
    }

    /**
     * Lets the workflows adopted by the claim of the given segment wake. The engine calls it once that claim started
     * its restored executions, so a wake can neither start a body a second time nor run one while the segment catches
     * up.
     *
     * @param segment the segment whose restored executions started, or {@code null} outside a segmented processor
     */
    void arm(@Nullable Segment segment) {
        if (segment == null) {
            return;
        }
        hibernationsOf(segment).values().forEach(hibernation -> hibernation.armed().complete(null));
    }

    /**
     * Forgets the hibernated workflows of the given released segment, so none of them wakes on this node. The next
     * claim of the segment, on any node, adopts them from the rows of its {@link WorkflowIndex}, or restores them.
     *
     * @param segment the released segment
     */
    void release(Segment segment) {
        segmentLock.lock();
        try {
            claims.remove(segment.getSegmentId());
            claimedSegments.remove(segment.getSegmentId());
        } finally {
            segmentLock.unlock();
        }
        // Outside the lock: dropping waits for wakes that are loading, and a hibernation must not wait for those.
        drop(hibernationsOf(segment));
    }

    /**
     * Keeps the given workflow out of memory until an awaited event, its deadline or a cancellation wakes it.
     */
    private void park(String workflowId,
                      @Nullable Segment segment,
                      Set<String> openWaits,
                      Instant wakesAt,
                      CompletableFuture<Void> armed) {
        var timer = scheduler.schedule(wakesAt);
        addHibernation(workflowId, new Hibernation(segment, Set.copyOf(openWaits), wakesAt, timer, armed));
        cancellationService.register(workflowId, new HibernatedWorkflowCancellation(workflowId));
        timer.completion().thenCompose(ignored -> wakeWhenArmed(workflowId, restored -> {
        }));
    }

    private boolean isInMemory(WorkflowExecution execution) {
        return repository.findById(execution.workflowId()).orElse(null) == execution;
    }

    /**
     * Returns the claimed segment that owns the given workflow, or {@code null} when none does. The caller holds the
     * segment lock.
     */
    private @Nullable Segment claimedSegmentOwning(String workflowId) {
        return claimedSegments.values()
                              .stream()
                              .filter(claimed -> WorkflowSegmentOwnership.ownedBy(claimed, workflowId))
                              .findFirst()
                              .orElse(null);
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
            var segmented = Segment.fromContext(execution.processingContext()).isPresent();
            segmentLock.lock();
            try {
                // The segment in its context may be one that a split or a merge replaced since, so the workflow is
                // parked under the claimed segment that owns it now.
                var segment = segmented ? claimedSegmentOwning(workflowId) : null;
                if (segmented && segment == null) {
                    // Its segment is being released, and the release stops the execution instead, or it is still
                    // being claimed, and a later pass hibernates it.
                    return false;
                }
                park(workflowId, segment, openWaits.get(), deadline.get(), CompletableFuture.completedFuture(null));
                var index = indexSupport.get();
                if (index != null) {
                    index.wakesAt(segment, workflowId, deadline.get());
                }
            } finally {
                segmentLock.unlock();
            }
            repository.remove(workflowId);
            checkpointWorkIndex.markSafe(workflowId);
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
                      Hibernation hibernation,
                      ProcessingContext executionContext,
                      Consumer<WorkflowExecution> beforeStart) {
        var resident = repository.findById(workflowId);
        if (resident.isPresent()) {
            // A later claim of its segment already loaded it, so the request goes to that execution.
            beforeStart.accept(resident.get());
        } else {
            loader.load(workflowId, executionContext, hibernation.segment(), beforeStart);
        }
        removeHibernation(workflowId, hibernation);
        var index = indexSupport.get();
        if (index != null) {
            index.clearWakesAt(hibernation.segment(), workflowId);
        }
        hibernation.wakeTimer().cancel();
        // A wake held until its segment started finds the workflow gone and does nothing.
        hibernation.armed().complete(null);
        logger.debug("Workflow [{}] woken from hibernation.", workflowId);
    }

    /**
     * Wakes a hibernated workflow outside event handling, for its deadline or a cancellation request, on a wake thread.
     * A workflow a claim adopted wakes only once that claim started its restored executions.
     *
     * @return a future completing once the wake ran, or was found unnecessary
     */
    private CompletableFuture<Void> wakeWhenArmed(String workflowId, Consumer<WorkflowExecution> beforeStart) {
        var hibernation = hibernations.get(workflowId);
        if (hibernation == null) {
            return CompletableFuture.completedFuture(null);
        }
        return hibernation.armed().thenRunAsync(() -> wakeDetached(workflowId, beforeStart), wakeExecutor);
    }

    /**
     * Wakes a hibernated workflow on the calling wake thread. The restored execution runs on a unit of work of its own,
     * the way a claimed segment's executions do. It waits for the load, so a failed wake schedules its retry on this
     * thread rather than on the thread that times the load out.
     */
    private void wakeDetached(String workflowId, Consumer<WorkflowExecution> beforeStart) {
        var hibernation = hibernations.get(workflowId);
        if (hibernation == null) {
            return;
        }
        try {
            unitOfWorkFactory.create("wake-" + workflowId)
                             .executeWithResult(executionContext -> {
                                 var segment = hibernation.segment();
                                 if (segment != null) {
                                     executionContext.putResource(Segment.RESOURCE_KEY, segment);
                                 }
                                 try (var ignored = locks.obtainLock(workflowId)) {
                                     var current = hibernations.get(workflowId);
                                     if (current != null) {
                                         wake(workflowId, current, executionContext, beforeStart);
                                     }
                                 }
                                 return CompletableFuture.completedFuture(null);
                             })
                             .orTimeout(restoreTimeout.get().toMillis(), TimeUnit.MILLISECONDS)
                             .join();
        } catch (RuntimeException e) {
            logger.warn("Could not wake hibernated workflow [{}]. Trying again in {}.", workflowId, interval, e);
            retryWake(workflowId);
        }
    }

    /**
     * Replaces the timer of a hibernated workflow whose wake failed with one that wakes it again after
     * one hibernation interval, unless it was woken or dropped meanwhile.
     */
    private void retryWake(String workflowId) {
        if (!running) {
            return;
        }
        try (var ignored = locks.obtainLock(workflowId)) {
            var current = hibernations.get(workflowId);
            if (current == null) {
                return;
            }
            var retry = scheduler.schedule(clock.instant().plus(interval));
            current.wakeTimer().cancel();
            hibernations.put(workflowId, current.withWakeTimer(retry));
            retry.completion().thenCompose(retried -> wakeWhenArmed(workflowId, restored -> {
            }));
        }
    }

    private void scheduleNextPass() {
        if (!running) {
            return;
        }
        var pass = scheduler.schedule(clock.instant().plus(interval));
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
     * The hibernation of a workflow that left memory at a safe point, or that a claim adopted from its row.
     *
     * @param segment   the segment that owns it, or {@code null} outside a segmented processor
     * @param openWaits the step names of the open waits it hibernated with
     * @param wakesAt   its nearest deadline
     * @param wakeTimer the timer that wakes it at its nearest deadline
     * @param armed     completes once it may wake: at once when it hibernated here, and once its claim started the
     *                  restored executions when a claim adopted it
     */
    private record Hibernation(@Nullable Segment segment,
                               Set<String> openWaits,
                               Instant wakesAt,
                               WorkflowScheduler.ScheduledTask wakeTimer,
                               CompletableFuture<Void> armed) {

        private Hibernation withWakeTimer(WorkflowScheduler.ScheduledTask newWakeTimer) {
            return new Hibernation(segment, openWaits, wakesAt, newWakeTimer, armed);
        }
    }

    /**
     * A claim of a segment that is still loading. It is compared by identity, so a claim that timed out never passes
     * for the next claim of the same segment.
     */
    static final class SegmentClaim {

        private SegmentClaim() {
        }
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
                if (inMemory == execution || (inMemory == null && !hibernations.containsKey(workflowId))) {
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
            var ready = new CompletableFuture<Void>();
            // Attached before the wake, so the request runs in the task that completes ready.
            var result = ready.thenCompose(ignored -> request.get());
            var queued = new AtomicBoolean();
            wakeWhenArmed(workflowId, restored -> {
                queued.set(true);
                restored.appendTask(ignored -> ready.complete(null));
            }).whenComplete((ignored, failure) -> {
                if (queued.get()) {
                    return;
                }
                if (failure != null) {
                    ready.completeExceptionally(failure);
                } else if (hibernations.containsKey(workflowId)) {
                    ready.completeExceptionally(new IllegalStateException(
                            "Workflow '" + workflowId + "' could not be woken from hibernation"
                    ));
                } else {
                    // Another wake restored it first, so queue the request there too, or it ended elsewhere and the
                    // cancellation service answers for it.
                    repository.findById(workflowId).ifPresentOrElse(
                            restored -> restored.appendTask(task -> ready.complete(null)),
                            () -> ready.complete(null)
                    );
                }
            });
            return result;
        }
    }
}
