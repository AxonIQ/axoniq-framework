/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepWaitInfo;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.OpenWait;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.Registration;
import org.axonframework.common.lock.LockFactory;
import org.axonframework.common.lock.PessimisticLockFactory;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Hibernates the idle workflows of a {@link WorkflowEngine}: a workflow that only waits for an external event or a
 * deadline leaves memory, and is restored from the event store when that event arrives, that deadline expires, or a
 * cancellation is requested for it.
 * <p>
 * Residency follows the {@link LiveWorkflows} registry: a workflow is in memory while the registry holds its execution,
 * which leaves memory through the registry when it hibernates, and enters it through the registry when it wakes. A
 * lock per workflow id, from a {@link PessimisticLockFactory}, keeps hibernation, event delivery, cancellation and
 * wakes of one workflow apart, so an event or a cancellation either reaches the execution in memory or finds the
 * workflow hibernated, never neither. The execution itself only reports whether it is at a safe point, through its
 * {@link WorkflowExecutionHibernationSupport}.
 * <p>
 * Every half idle delay, a pass asks the running executions in memory that made no progress for the idle delay to
 * hibernate. A hibernated workflow keeps a timer for its nearest deadline, a stand-in {@link WorkflowCancellation}, and
 * the rows of its open waits in a {@link WaitAssociationIndex}, which routes broadcast events to it. No event is written:
 * a hibernating workflow records its {@link HibernationHint hint}, its open waits and nearest deadline, in the
 * {@link RunningWorkflowsProjection}, which writes it to the workflow index with a checkpoint that joins the
 * processor's transaction, and clears it again when the workflow wakes. Releasing or claiming a segment forgets the
 * hibernated workflows of that segment. A claim of the segment, here or on another node, restores every running
 * workflow of the segment, except those whose hint it accepts: it {@link #adopt(SegmentClaim, String, HibernationHint)
 * adopts} those as hibernated workflows instead, which wake for an event like any other, but for their deadline or a
 * cancellation only once the claim caught up.
 * <p>
 * A wake restores the workflow from the event store in a unit of work of its own, on the execution context of the
 * claim that holds its segment, and hands it what woke it before its body starts. A wake by an event runs on the
 * thread handling the event, bounded by the wake timeout. A failed wake by an event fails the event: the engine stores
 * no position past it and asks the event processor to release the segment, so the next claim restores the workflow and
 * handles the event again. A wake by a deadline or a cancellation runs on the wake executor, at most
 * {@value #MAX_CONCURRENT_DETACHED_WAKES} at a time, so the deadlines that passed while a segment was not processed do
 * not all load at once, and is tried again after the idle delay when it fails.
 * <p>
 * The {@link WorkflowEngine} creates it when hibernation is enabled for its workflow module, starts it with the engine
 * and stops it on shutdown.
 *
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 5.5.0
 */
final class WorkflowEngineHibernationSupport {

    /**
     * The number of wakes for a deadline or a cancellation that load a workflow at the same time.
     */
    static final int MAX_CONCURRENT_DETACHED_WAKES = 16;
    private static final Duration MIN_PASS_INTERVAL = Duration.ofMillis(10);
    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineHibernationSupport.class);

    private final WorkflowScheduler scheduler;
    private final Clock clock;
    private final LiveWorkflows liveWorkflows;
    private final WorkflowCancellationService cancellationService;
    private final RunningWorkflowsProjection projection;
    private final WorkflowLoader loader;
    private final Duration idleDelay;
    private final Executor wakeExecutor;
    private final LockFactory locks = PessimisticLockFactory.usingDefaults();
    private final Map<String, Hibernation> hibernations = new ConcurrentHashMap<>();
    private final WaitAssociationIndex waitIndex = new WaitAssociationIndex();
    private final Semaphore detachedWakes = new Semaphore(MAX_CONCURRENT_DETACHED_WAKES);
    // The cancellation requests of hibernated workflows queued on the executions that woke for them, until they run.
    private final Map<WorkflowExecution, Set<CompletableFuture<Void>>> queuedRequests = new ConcurrentHashMap<>();
    // The broadcasts and manager queries looking up hibernated workflows, which a wake tells the workflow it woke.
    private final Set<Consumer<String>> wakeListeners = ConcurrentHashMap.newKeySet();
    // Cleared on stop, so no workflow hibernates after the hibernated ones are dropped.
    private volatile boolean running;
    private volatile WorkflowScheduler.@Nullable ScheduledTask nextPass;

    /**
     * Creates the hibernation support of an engine.
     *
     * @param scheduler           schedules the passes and the deadlines of hibernated workflows
     * @param clock               the clock the scheduler measures deadlines with
     * @param liveWorkflows       the workflows in memory, which a workflow leaves when it hibernates
     * @param cancellationService the cancellation registrations of the live workflows
     * @param projection          records the hints of hibernated workflows for the workflow index
     * @param loader              loads a hibernated workflow back into the engine and starts it
     * @param idleDelay           the time a workflow must make no progress before it hibernates, and before a failed
     *                            wake by a deadline or a cancellation is tried again
     * @param wakeExecutor        runs the wakes for a deadline or a cancellation, which load from the store, so they
     *                            never run on the timer thread
     */
    WorkflowEngineHibernationSupport(WorkflowScheduler scheduler,
                                     Clock clock,
                                     LiveWorkflows liveWorkflows,
                                     WorkflowCancellationService cancellationService,
                                     RunningWorkflowsProjection projection,
                                     WorkflowLoader loader,
                                     Duration idleDelay,
                                     Executor wakeExecutor) {
        this.scheduler = Objects.requireNonNull(scheduler, "The WorkflowScheduler must not be null.");
        this.clock = Objects.requireNonNull(clock, "The Clock must not be null.");
        this.liveWorkflows = Objects.requireNonNull(liveWorkflows, "The LiveWorkflows must not be null.");
        this.cancellationService = Objects.requireNonNull(cancellationService,
                                                          "The WorkflowCancellationService must not be null.");
        this.projection = Objects.requireNonNull(projection, "The RunningWorkflowsProjection must not be null.");
        this.loader = Objects.requireNonNull(loader, "The WorkflowLoader must not be null.");
        this.idleDelay = Objects.requireNonNull(idleDelay, "The idle delay must not be null.");
        this.wakeExecutor = Objects.requireNonNull(wakeExecutor, "The wake Executor must not be null.");
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
    }

    /**
     * Asks every running execution in memory that made no progress for the idle delay to hibernate at its next safe
     * point.
     */
    void pass() {
        for (var execution : liveWorkflows.inMemory()) {
            try {
                if (execution instanceof WorkflowHibernationProvider provider) {
                    provider.workflowHibernation().request(idleDelay);
                }
            } catch (RuntimeException e) {
                logger.warn("Could not ask workflow [{}] to hibernate.", execution.workflowId(), e);
            }
        }
    }

    /**
     * Enables hibernation for the given execution, which the engine just brought into memory under the given claim.
     * The execution only hibernates while that claim is current: once a later claim of its segment started, or the
     * segment was released, it stays in memory until the engine stops it.
     *
     * @param execution the execution that may hibernate from now on
     * @param claim     the claim the execution was started or restored under, or {@code null} when it has none, in
     *                  which case it never hibernates
     */
    void allow(WorkflowExecution execution, @Nullable SegmentClaim claim) {
        if (claim != null && claim.executionContext() != null
                && execution instanceof WorkflowHibernationProvider provider) {
            provider.workflowHibernation().enable((hibernating, safePoint) -> hibernate(hibernating, claim, safePoint));
        }
    }

    /**
     * Runs the given action under the lock of the given workflow, unless the workflow is hibernated here. The engine
     * starts a new workflow this way, so the workflow cannot hibernate between the check for a duplicate start and the
     * start.
     *
     * @param workflowId the identifier of the workflow
     * @param action     the action to run when the workflow is not hibernated
     * @return {@code false} when the workflow is hibernated, so the action did not run
     */
    boolean unlessHibernated(String workflowId, Runnable action) {
        try (var ignored = locks.obtainLock(workflowId)) {
            if (hibernations.containsKey(workflowId)) {
                return false;
            }
            action.run();
            return true;
        }
    }

    /**
     * Delivers a broadcast event to the workflows of the given segment: to every execution in memory, and to the
     * hibernated workflows with an open wait that may match the event, which it wakes. A failed delivery to one
     * workflow does not keep the event from the others: it is delivered to every workflow, after which the first
     * failure is rethrown.
     *
     * @param segment the segment handling the event, or {@code null} outside a segmented processor
     * @param event   the event to deliver
     * @param context the context of the event
     */
    void broadcast(@Nullable Segment segment, EventMessage event, ProcessingContext context) {
        // A workflow that hibernates after the first match of the waits and wakes before the second, having left memory
        // before the executions are listed, is in neither lookup: its wake reports it to this listener.
        var woken = ConcurrentHashMap.<String>newKeySet();
        var wakes = onWake(woken::add);
        Set<String> workflowIds;
        try {
            workflowIds = addressees(segment, event, context);
        } finally {
            wakes.cancel();
        }
        woken.stream()
             .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
             .forEach(workflowIds::add);
        deliverToEach(workflowIds, event, context);
    }

    /**
     * Returns the workflows of the given segment a broadcast event goes to: every execution in memory, and every
     * hibernated workflow with an open wait that may match the event.
     */
    private Set<String> addressees(@Nullable Segment segment, EventMessage event, ProcessingContext context) {
        // Matched before the executions are listed: a workflow wakes by entering memory, which stores its execution
        // in the repository the listing reads, before its waits are removed, so a workflow that wakes while the
        // executions are listed is found here.
        var awaiting = new LinkedHashSet<>(waitIndex.match(event, context));
        var workflowIds = new LinkedHashSet<String>();
        liveWorkflows.inMemoryOwnedBy(segment).forEach(execution -> workflowIds.add(execution.workflowId()));
        // Matched after the executions are listed: a workflow hibernates by adding its waits before it leaves memory,
        // which removes its execution from the repository the listing reads, so a workflow that hibernates while the
        // executions are listed is found here.
        awaiting.addAll(waitIndex.match(event, context));
        awaiting.stream()
                .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                .forEach(workflowIds::add);
        return workflowIds;
    }

    /**
     * Delivers a broadcast event to each of the given workflows, and rethrows the first failure once it was delivered
     * to every one of them.
     */
    private void deliverToEach(Set<String> workflowIds, EventMessage event, ProcessingContext context) {
        RuntimeException failure = null;
        for (var workflowId : workflowIds) {
            try {
                deliver(workflowId, event, context, true);
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Delivers an event of the given workflow to it: to its execution in memory, or, when it is hibernated, to the
     * workflow restored from the event store.
     *
     * @param workflowId the identifier of the workflow the event is for
     * @param event      the event to deliver
     * @param context    the context of the event
     * @return {@code false} when the workflow is neither in memory nor hibernated here, so it belongs to another
     * engine, which is expected in multi-module setups
     */
    boolean deliver(String workflowId, EventMessage event, ProcessingContext context) {
        return deliver(workflowId, event, context, false);
    }

    private boolean deliver(String workflowId, EventMessage event, ProcessingContext context, boolean whenAwaited) {
        try (var ignored = locks.obtainLock(workflowId)) {
            var execution = liveWorkflows.find(workflowId);
            if (execution.isPresent()) {
                execution.get().onEvent(event, context);
                return true;
            }
            var hibernation = hibernations.get(workflowId);
            if (hibernation == null) {
                return false;
            }
            if (!whenAwaited || waitIndex.matches(workflowId, event, context)) {
                wakeForEvent(workflowId, hibernation, event, context);
            }
            return true;
        }
    }

    /**
     * Wakes the given workflow on the thread handling the event. A failure fails the event, so the event processor
     * hands it to the workflow again. The caller holds the lock of the workflow.
     */
    private void wakeForEvent(String workflowId,
                              Hibernation hibernation,
                              EventMessage event,
                              ProcessingContext context) {
        try {
            wake(workflowId, hibernation, restored -> restored.onEvent(event, context));
        } catch (RuntimeException e) {
            logger.warn("Could not wake hibernated workflow [{}] for event [{}]; failing the event so it is delivered "
                                + "again.",
                        workflowId, event.type(), e);
            throw e;
        }
    }

    /**
     * Registers a listener that every wake tells the identifier of the workflow it woke, once the workflow is in memory
     * and before it leaves the hibernated workflows.
     * <p>
     * A lookup that checks the hibernated workflows both before and after it lists the executions in memory registers
     * one for its duration: a workflow that hibernates after the first check and wakes before the second is in none of
     * the three, but reports its wake to the listener.
     *
     * @param listener the listener to tell the identifier of each workflow that wakes, on the thread that wakes it
     * @return the registration that removes the listener again
     */
    Registration onWake(Consumer<String> listener) {
        Objects.requireNonNull(listener, "The wake listener must not be null.");
        wakeListeners.add(listener);
        return () -> wakeListeners.remove(listener);
    }

    /**
     * Returns the identifiers of the workflows hibernated here, as a view that follows the workflows that hibernate and
     * wake, so checking whether a workflow is among them copies nothing. Callers must not modify it.
     *
     * @return a view of the identifiers of the hibernated workflows
     */
    Set<String> hibernatedWorkflowIds() {
        return hibernations.keySet();
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
     * Answers the cancellation requests that wait in the task queue of the given execution, which left memory, so it
     * runs none of them. Without a failure, each request is made again through the
     * {@link WorkflowCancellationService}: it reaches the workflow wherever it runs now, hibernated again, restored by
     * a claim, or nowhere on this node, in which case it fails with a {@link java.util.NoSuchElementException}. With a
     * failure, which made a wake fail, each request fails with it.
     * <p>
     * The requests are answered on the wake executor, never on the calling thread: an execution may leave memory under
     * the lock of a claim, and a request made again takes the lock of its workflow, which the engine otherwise takes
     * before the lock of a claim. Only when the wake executor no longer accepts tasks, on shutdown, are they answered
     * on the calling thread.
     *
     * @param execution the execution that left memory
     * @param failure   the failure that made the execution leave memory, or {@code null} when it left otherwise
     */
    void leftMemory(WorkflowExecution execution, @Nullable Throwable failure) {
        var queued = queuedRequests.remove(execution);
        if (queued == null) {
            return;
        }
        Runnable answer = () -> queued.forEach(ready -> {
            if (failure == null) {
                ready.complete(null);
            } else {
                ready.completeExceptionally(failure);
            }
        });
        try {
            wakeExecutor.execute(answer);
        } catch (RejectedExecutionException e) {
            answer.run();
        }
    }

    /**
     * Queues the given request as a task of the given restored execution, the way an event that wakes a workflow is
     * queued, and remembers it until it runs, so it is answered when the execution leaves memory first.
     */
    private void queueRequest(WorkflowExecution restored, CompletableFuture<Void> ready) {
        queuedRequests.computeIfAbsent(restored, ignored -> ConcurrentHashMap.newKeySet()).add(ready);
        ready.whenComplete((result, failure) -> queuedRequests.computeIfPresent(restored, (ignored, requests) -> {
            requests.remove(ready);
            return requests.isEmpty() ? null : requests;
        }));
        restored.appendTask(ignored -> ready.complete(null));
        if (!liveWorkflows.isInMemory(restored)) {
            // It left before the request was remembered, or dropped the task as it is stopped.
            leftMemory(restored, null);
        }
    }

    /**
     * Leaves the given workflow, whose hint a claim accepted, out of memory as a hibernated workflow of that claim,
     * instead of restoring it.
     * <p>
     * The workflow wakes for an awaited event or one of its own like any hibernated workflow, also while the claim
     * catches up, in which case its body starts once the claim caught up. Its deadline and a cancellation only wake it
     * once the claim caught up: see {@link #arm(Segment)}.
     *
     * @param claim      the claim that accepted the hint
     * @param workflowId the identifier of the hibernated workflow
     * @param hint       the open waits and nearest deadline the workflow hibernated with
     * @return {@code false} when the workflow was not adopted, as hibernation stopped, the claim is no longer current,
     * or the workflow is in memory or hibernated already
     */
    boolean adopt(SegmentClaim claim, String workflowId, HibernationHint hint) {
        try (var ignored = locks.obtainLock(workflowId)) {
            if (!running || hibernations.containsKey(workflowId) || liveWorkflows.find(workflowId).isPresent()) {
                return false;
            }
            return claim.startWhileCurrent(mayStartBodies -> park(
                    workflowId, claim, hint,
                    mayStartBodies ? CompletableFuture.completedFuture(null) : new CompletableFuture<>()
            ));
        }
    }

    /**
     * Lets the workflows adopted by the claim of the given segment wake for their deadline or a cancellation, as the
     * claim caught up. A deadline that passed meanwhile wakes its workflow right away.
     *
     * @param segment the segment whose claim caught up
     */
    void arm(Segment segment) {
        hibernations.values().forEach(hibernation -> {
            if (hibernation.claim().segment().equals(segment)) {
                hibernation.armed().complete(null);
            }
        });
    }

    /**
     * Forgets the hibernated workflows of the given claim, whose restore failed. Leaves those of a later claim of the
     * same segment in place.
     *
     * @param claim the claim that failed
     */
    void drop(SegmentClaim claim) {
        var dropped = new HashMap<String, Hibernation>();
        hibernations.forEach((workflowId, hibernation) -> {
            if (hibernation.claim() == claim) {
                dropped.put(workflowId, hibernation);
            }
        });
        drop(dropped);
    }

    /**
     * Forgets the hibernated workflows of the given segment, and of every segment overlapping it, which a split or a
     * merge replaced without a release. Their durable state is untouched: the next claim of their segment restores
     * them.
     *
     * @param segment the segment that is released or claimed
     */
    void drop(Segment segment) {
        var dropped = new HashMap<String, Hibernation>();
        hibernations.forEach((workflowId, hibernation) -> {
            if (WorkflowSegmentOwnership.overlap(hibernation.claim().segment(), segment)) {
                dropped.put(workflowId, hibernation);
            }
        });
        drop(dropped);
    }

    /**
     * Forgets the given hibernated workflows and cancels their deadlines. Takes the lock of each workflow, so a wake
     * that is loading finishes first.
     */
    private void drop(Map<String, Hibernation> dropped) {
        dropped.forEach((workflowId, hibernation) -> {
            try (var ignored = locks.obtainLock(workflowId)) {
                if (forget(workflowId, hibernation)) {
                    cancellationService.unregister(workflowId, hibernation.cancellation());
                }
            }
        });
    }

    /**
     * Removes the given hibernation of the given workflow, unless another one replaced it, with its wait rows and its
     * deadline. The caller holds the lock of the workflow.
     *
     * @return {@code true} when the given hibernation was removed
     */
    private boolean forget(String workflowId, Hibernation hibernation) {
        if (!hibernations.remove(workflowId, hibernation)) {
            return false;
        }
        waitIndex.removeWorkflow(workflowId);
        hibernation.wakeTimer().cancel();
        // A wake held until the hibernation may wake finds the workflow gone and does nothing.
        hibernation.armed().complete(null);
        return true;
    }

    /**
     * Records the given execution as hibernated and removes it from memory, when it is at a safe point. Runs on the
     * workflow thread, under the lock of the workflow, so no event or cancellation can reach the execution between the
     * safe point check and its removal, and under the lock of the claim the execution was started or restored
     * under, so a workflow never hibernates under a claim that ended, and never under a later claim, which stops the
     * executions of its predecessor.
     *
     * @return {@code false}, keeping the workflow in memory, when it is not at a safe point, when one of its waits has
     * no readable timeout, because nothing could wake that wait again, or when its claim is no longer current
     */
    private boolean hibernate(WorkflowExecution execution,
                              SegmentClaim claim,
                              Supplier<Optional<Set<String>>> safePoint) {
        var workflowId = execution.workflowId();
        try (var ignored = locks.obtainLock(workflowId)) {
            if (!running || !liveWorkflows.isInMemory(execution)) {
                return false;
            }
            var openWaits = safePoint.get();
            if (openWaits.isEmpty()) {
                return false;
            }
            var waits = readWaits(execution, openWaits.get());
            if (waits.isEmpty()) {
                logger.debug("Not hibernating workflow [{}]: not each of its waits {} has a readable timeout.",
                             workflowId, openWaits.get());
                return false;
            }
            var deadline = waits.get().values().stream()
                                .map(StepWaitInfo::timeoutTime)
                                .min(Instant::compareTo)
                                .orElseThrow();
            var hint = hintOf(waits.get(), deadline);
            boolean parked = claim.runWhileCurrent(() -> {
                park(workflowId, claim, hint, CompletableFuture.completedFuture(null));
                projection.recordHibernated(claim.segment(), workflowId, hint);
                liveWorkflows.leave(execution);
            });
            if (parked) {
                // A request queued on the execution after its safe point was checked goes to the hibernation now.
                leftMemory(execution, null);
                logger.debug("Workflow [{}] hibernated until an awaited event or its deadline at {}.",
                             workflowId, deadline);
            }
            return parked;
        }
    }

    /**
     * Returns the wait info of the given open waits, read the way {@link WaitForDelegate} reads it: the result of the
     * wait's STARTED step, converted to the {@link StepWaitInfo} the wait published.
     *
     * @return the wait info by step name, or empty when one of the waits has no readable timeout
     */
    private static Optional<Map<String, StepWaitInfo>> readWaits(WorkflowExecution execution, Set<String> openWaits) {
        var converter = execution.processingContext().component(EventConverter.class);
        var state = execution.state();
        var waits = new HashMap<String, StepWaitInfo>();
        for (var stepName : openWaits) {
            var step = state.getStep(stepName);
            var info = step == null || converter == null ? null : converter.convert(step.result(), StepWaitInfo.class);
            if (info == null || info.timeoutTime() == null) {
                return Optional.empty();
            }
            waits.put(stepName, info);
        }
        return Optional.of(waits);
    }

    /**
     * Returns the hint of a workflow hibernating with the given open waits until the given deadline.
     */
    private static HibernationHint hintOf(Map<String, StepWaitInfo> waits, Instant deadline) {
        List<OpenWait> openWaits = new ArrayList<>();
        waits.forEach((stepName, info) -> openWaits.add(new OpenWait(
                stepName,
                info.eventName() == null || info.eventName().isEmpty() ? null : info.eventName(),
                Objects.requireNonNullElse(info.associations(), Set.of())
        )));
        return new HibernationHint(deadline, openWaits);
    }

    /**
     * Keeps the given workflow out of memory until an awaited event, its deadline or a cancellation wakes it, the
     * latter two only once the given {@code armed} future completed. The caller holds the lock of the workflow.
     */
    private void park(String workflowId, SegmentClaim claim,
                      HibernationHint hint, CompletableFuture<Void> armed) {
        var timer = scheduler.schedule(hint.wakeAt());
        var cancellation = new HibernatedWorkflowCancellation(workflowId);
        hibernations.put(workflowId, new Hibernation(claim, timer, armed, cancellation));
        hint.waits().forEach(wait -> waitIndex.add(
                workflowId,
                wait.stepName(),
                wait.eventName() == null ? null : new QualifiedName(wait.eventName()),
                wait.associations()
        ));
        cancellationService.register(workflowId, cancellation);
        timer.completion().thenCompose(ignored -> wakeWhenArmed(workflowId, restored -> {
        }));
    }

    /**
     * Restores a hibernated workflow from the event store and starts it. The caller holds the lock of the workflow.
     * <p>
     * The given action runs on the restored execution before its body starts, so the body cannot hibernate again
     * before it sees what woke it.
     */
    private void wake(String workflowId, Hibernation hibernation, Consumer<WorkflowExecution> beforeStart) {
        boolean loaded;
        try {
            loaded = loader.load(hibernation.claim(), workflowId, beforeStart);
        } catch (RuntimeException e) {
            // The execution that failed to start may have registered its cancellation in place of the stand-in.
            if (hibernations.get(workflowId) == hibernation) {
                cancellationService.register(workflowId, hibernation.cancellation());
            }
            throw e;
        }
        if (loaded) {
            // In memory now, but not yet out of the hibernated workflows: a broadcast or a manager query that listed
            // the executions before and looks the hibernated workflows up after this moment learns of it here.
            wakeListeners.forEach(listener -> listener.accept(workflowId));
        }
        forget(workflowId, hibernation);
        if (loaded) {
            projection.recordWoken(hibernation.claim().segment(), workflowId);
            logger.debug("Workflow [{}] woken from hibernation.", workflowId);
        } else {
            // It ended elsewhere, or its claim ended meanwhile: its stand-in has nothing left to stand in for.
            cancellationService.unregister(workflowId, hibernation.cancellation());
            logger.debug("Workflow [{}] did not need waking: it ended, or the claim of its segment did.", workflowId);
        }
    }

    /**
     * Wakes a hibernated workflow outside event handling, for its deadline or a cancellation request, on a wake thread,
     * once the hibernation may wake.
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
     * Wakes a hibernated workflow on the calling wake thread, once fewer than
     * {@value #MAX_CONCURRENT_DETACHED_WAKES} such wakes load a workflow. A failed wake schedules its retry after the
     * idle delay.
     */
    private void wakeDetached(String workflowId, Consumer<WorkflowExecution> beforeStart) {
        if (!hibernations.containsKey(workflowId)) {
            // Woken or dropped meanwhile: no need to wait for a permit only to find that out under the lock.
            return;
        }
        detachedWakes.acquireUninterruptibly();
        try (var ignored = locks.obtainLock(workflowId)) {
            var current = hibernations.get(workflowId);
            if (current != null) {
                wake(workflowId, current, beforeStart);
            }
        } catch (RuntimeException e) {
            logger.warn("Could not wake hibernated workflow [{}]. Trying again in {}.", workflowId, idleDelay, e);
            retryWake(workflowId);
        } finally {
            detachedWakes.release();
        }
    }

    /**
     * Returns the number of wakes for a deadline or a cancellation that wait for one of the
     * {@value #MAX_CONCURRENT_DETACHED_WAKES} permits to load a workflow. Package-private so a test can tell that every
     * wake either loads a workflow or waits for a permit, instead of inspecting the states of their threads.
     *
     * @return the number of wakes waiting for a permit
     */
    int wakesWaitingForAPermit() {
        return detachedWakes.getQueueLength();
    }

    /**
     * Replaces the timer of a hibernated workflow whose wake failed with one that wakes it again after the idle delay,
     * unless it was woken or dropped meanwhile.
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
            var retry = scheduler.schedule(clock.instant().plus(idleDelay));
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
        var interval = idleDelay.dividedBy(2);
        var pass = scheduler.schedule(clock.instant().plus(
                interval.compareTo(MIN_PASS_INTERVAL) < 0 ? MIN_PASS_INTERVAL : interval
        ));
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
     * Loads a hibernated workflow back into the engine and starts it, on behalf of this support.
     */
    @FunctionalInterface
    interface WorkflowLoader {

        /**
         * Loads the workflow from the event store into memory under the given claim and starts it once the claim
         * caught up, after handing the loaded execution to the given action. Fails when the workflow cannot be loaded
         * in time.
         *
         * @param claim       the claim the workflow hibernated under
         * @param workflowId  the identifier of the hibernated workflow
         * @param beforeStart receives the loaded execution before its body starts
         * @return {@code false} when there was nothing to load: the workflow ended elsewhere, or the claim ended
         */
        boolean load(SegmentClaim claim, String workflowId,
                     Consumer<WorkflowExecution> beforeStart);
    }

    /**
     * The hibernation of a workflow that left memory at a safe point.
     *
     * @param claim        the claim of the segment that owns it, which it wakes under
     * @param wakeTimer    the timer that wakes it at its nearest deadline
     * @param armed        completes once it may wake for its deadline or a cancellation
     * @param cancellation the stand-in registered for its cancellation
     */
    private record Hibernation(SegmentClaim claim,
                               WorkflowScheduler.ScheduledTask wakeTimer,
                               CompletableFuture<Void> armed,
                               WorkflowCancellation cancellation) {

        private Hibernation withWakeTimer(WorkflowScheduler.ScheduledTask newWakeTimer) {
            return new Hibernation(claim, newWakeTimer, armed, cancellation);
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
            return whileInMemory(cancellation -> cancellation.requestStepCancellation(stepName, cause));
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return whileInMemory(cancellation -> cancellation.requestCancellationOfAllSteps(cause));
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return whileInMemory(cancellation -> cancellation.requestWorkflowCancellation(cause));
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

        /**
         * Runs the given request against the execution while it is in memory, against the stand-in of the hibernation
         * while the workflow is hibernated, and otherwise against the registration of the execution that replaced
         * this one. Never against this cancellation itself, which would recurse.
         */
        private <T> CompletableFuture<T> whileInMemory(Function<WorkflowCancellation, CompletableFuture<T>> request) {
            var workflowId = execution.workflowId();
            try (var ignored = locks.obtainLock(workflowId)) {
                var inMemory = liveWorkflows.find(workflowId).orElse(null);
                var hibernation = hibernations.get(workflowId);
                if (inMemory == execution || (inMemory == null && hibernation == null)) {
                    return request.apply(delegate);
                }
                if (inMemory == null) {
                    return request.apply(hibernation.cancellation());
                }
            }
            var replacement = cancellationService.registered(workflowId);
            if (replacement == null || replacement == this) {
                return CompletableFuture.failedFuture(new NoSuchElementException(
                        "No running workflow found with id '" + workflowId + "'"
                ));
            }
            return request.apply(replacement);
        }
    }

    /**
     * Stands in for the cancellation of a hibernated workflow. It wakes the workflow and queues the request as a task
     * of the restored execution before its body starts, the way an event that wakes a workflow is queued.
     * <p>
     * A restored body takes no task before it waits again where it hibernated, because every earlier wait is answered
     * from its recorded state. So the request runs once the body replayed its steps and they are registered again. When
     * the restored execution leaves memory before it ran the request, the request is made again through the
     * {@link WorkflowCancellationService}, see {@link #leftMemory(WorkflowExecution, Throwable)}.
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
                queueRequest(restored, ready);
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
                    liveWorkflows.find(workflowId).ifPresentOrElse(
                            restored -> queueRequest(restored, ready),
                            () -> ready.complete(null)
                    );
                }
            });
            return result;
        }
    }
}
