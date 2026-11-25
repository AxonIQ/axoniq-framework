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
import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.Hibernated;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import io.axoniq.framework.workflow.runtime.execution.SegmentClaim.BodyStart;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Hibernates the idle workflows of a {@link WorkflowEngine}: a workflow that only waits for an external event or a
 * deadline leaves memory, and is restored from the event store when that event arrives, that deadline expires, or a
 * cancellation is requested for it.
 * <p>
 * This support is the hibernation of an engine whose module enables it: it decides when a workflow hibernates and
 * wakes, builds the {@link HibernationHint hint} and the timer of a hibernation, and throttles the wakes that do not
 * run for an event. It keeps no residency of its own. The {@link LiveWorkflows} registry keeps every live workflow, in
 * memory or hibernated, with the rows of the open waits of the hibernated ones, and applies the transitions this
 * support decides on: a workflow hibernates by swapping its execution in memory for a {@link Hibernated hibernation},
 * and wakes by swapping it back. The lock of each workflow, from the registry, keeps hibernation, event delivery,
 * cancellation and wakes of one workflow apart, so an event or a cancellation either reaches the execution in memory or
 * finds the workflow hibernated, never neither. The execution itself only reports whether it is at a safe point,
 * through its {@link WorkflowExecutionHibernationSupport}.
 * <p>
 * Every half idle delay, a pass asks the running executions in memory that made no progress for the idle delay to
 * hibernate. A hibernated workflow keeps a timer for its nearest deadline, and the registry keeps the rows of its open
 * waits, which route broadcast events to it, and finds it for a cancellation request, which the
 * {@link WorkflowCancellationService} has this support wake it for, see
 * {@link #wakeForCancellation(String, Consumer)}. The registry never calls this support: the engine hands
 * it the {@link #wakeForEvent(Hibernated, EventMessage, ProcessingContext) wake for an event} as it delivers one. No
 * event is written: a hibernating workflow records its {@link HibernationHint hint}, its open waits and nearest
 * deadline, in the {@link RunningWorkflowsProjection}, which writes it to the workflow index with a checkpoint that
 * joins the processor's transaction, and clears it again when the workflow wakes. Releasing or claiming a segment
 * forgets the hibernated workflows of that segment. A claim of the segment, here or on another node, restores every
 * running workflow of the segment, except those whose hint it accepts: it
 * {@link #adopt(SegmentClaim, String, HibernationHint) adopts} those as hibernated workflows instead, which wake for an
 * event like any other, but for their deadline or a cancellation only once the claim caught up.
 * <p>
 * A wake restores the workflow from the event store in a unit of work of its own, on the execution context of the
 * claim that holds its segment, and hands it what woke it before its body starts. A wake by an event runs on the
 * thread handling the event, bounded by the wake timeout. A failed wake by an event fails the event: the engine stores
 * no position past it and asks the event processor to release the segment, so the next claim restores the workflow and
 * handles the event again. A wake by a deadline or a cancellation runs on the wake executor, at most
 * {@value #MAX_CONCURRENT_DETACHED_WAKES} at a time, so the deadlines that passed while a segment was not processed do
 * not all load at once, and is tried again after the idle delay when it fails.
 * <p>
 * The {@link WorkflowEngine} creates it as its {@link Hibernation} when hibernation is enabled for its workflow module,
 * starts it with the engine and stops it on shutdown.
 *
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 5.5.0
 */
final class WorkflowEngineHibernationSupport implements Hibernation {

    /**
     * The number of wakes for a deadline or a cancellation that load a workflow at the same time.
     */
    static final int MAX_CONCURRENT_DETACHED_WAKES = 16;
    private static final Duration MIN_PASS_INTERVAL = Duration.ofMillis(10);
    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineHibernationSupport.class);

    private final WorkflowScheduler scheduler;
    private final Clock clock;
    private final LiveWorkflows liveWorkflows;
    private final RunningWorkflowsProjection projection;
    private final WorkflowLoader loader;
    private final Duration idleDelay;
    private final Executor wakeExecutor;
    private final Semaphore detachedWakes = new Semaphore(MAX_CONCURRENT_DETACHED_WAKES);
    // Cleared on stop, so no workflow hibernates after the hibernated ones are dropped.
    private volatile boolean running;
    private volatile WorkflowScheduler.@Nullable ScheduledTask nextPass;

    /**
     * Creates the hibernation support of an engine.
     *
     * @param scheduler     schedules the passes and the deadlines of hibernated workflows
     * @param clock         the clock the scheduler measures deadlines with
     * @param liveWorkflows the live workflows, which apply the transitions between memory and hibernation
     * @param projection    records the hints of hibernated workflows for the workflow index
     * @param loader        loads a hibernated workflow back into the engine and starts it
     * @param idleDelay     the time a workflow must make no progress before it hibernates, and before a failed wake by
     *                      a deadline or a cancellation is tried again
     * @param wakeExecutor  runs the wakes for a deadline or a cancellation, which load from the store, so they never
     *                      run on the timer thread
     */
    WorkflowEngineHibernationSupport(WorkflowScheduler scheduler,
                                     Clock clock,
                                     LiveWorkflows liveWorkflows,
                                     RunningWorkflowsProjection projection,
                                     WorkflowLoader loader,
                                     Duration idleDelay,
                                     Executor wakeExecutor) {
        this.scheduler = Objects.requireNonNull(scheduler, "The WorkflowScheduler must not be null.");
        this.clock = Objects.requireNonNull(clock, "The Clock must not be null.");
        this.liveWorkflows = Objects.requireNonNull(liveWorkflows, "The LiveWorkflows must not be null.");
        this.projection = Objects.requireNonNull(projection, "The RunningWorkflowsProjection must not be null.");
        this.loader = Objects.requireNonNull(loader, "The WorkflowLoader must not be null.");
        this.idleDelay = Objects.requireNonNull(idleDelay, "The idle delay must not be null.");
        this.wakeExecutor = Objects.requireNonNull(wakeExecutor, "The wake Executor must not be null.");
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Visits every workflow live in the engine.
     */
    @Override
    public Set<String> hibernatedWorkflowIds() {
        return liveWorkflows.hibernatedWorkflowIds();
    }

    /**
     * Starts the passes.
     */
    @Override
    public void start() {
        running = true;
        scheduleNextPass();
    }

    /**
     * Stops the passes and forgets every hibernated workflow. A pass that is running finishes.
     */
    @Override
    public void stop() {
        running = false;
        var pass = nextPass;
        if (pass != null) {
            pass.cancel();
        }
        drop(liveWorkflows.hibernatedOwnedBy(null).toList());
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
     * {@inheritDoc}
     * <p>
     * An execution may hibernate under a claim with an execution context, which a wake restores it on. An execution
     * of an implicit claim, or outside a segmented processor, never hibernates.
     */
    @Override
    public boolean mayHibernate(@Nullable SegmentClaim claim) {
        return claim != null && claim.executionContext() != null;
    }

    /**
     * Enables hibernation for the given execution, which the engine just brought into memory under the given claim,
     * when it {@link #mayHibernate(SegmentClaim) may hibernate} under that claim. The execution only hibernates while
     * that claim is current: once a later claim of its segment started, or the segment was released, it stays in
     * memory until the engine stops it.
     *
     * @param execution the execution that may hibernate from now on
     * @param claim     the claim the execution was started or restored under, or {@code null} when it has none, in
     *                  which case it never hibernates
     */
    @Override
    public void allow(WorkflowExecution execution, @Nullable SegmentClaim claim) {
        if (mayHibernate(claim) && execution instanceof WorkflowHibernationProvider provider) {
            provider.workflowHibernation().enable((hibernating, safePoint) -> hibernate(hibernating, claim, safePoint));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * A failure fails the event, so the event processor hands it to the workflow again.
     */
    @Override
    public void wakeForEvent(Hibernated hibernated, EventMessage event, ProcessingContext context) {
        try {
            wake(hibernated, restored -> restored.onEvent(event, context));
        } catch (RuntimeException e) {
            logger.warn("Could not wake hibernated workflow [{}] for event [{}]; failing the event so it is delivered "
                                + "again.",
                        hibernated.workflowId(), event.type(), e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Wakes the workflow on the wake executor, like a wake for its deadline. A failed wake is tried again after the
     * idle delay.
     */
    @Override
    public CompletableFuture<Void> wakeForCancellation(String workflowId, Consumer<WorkflowExecution> beforeStart) {
        return wakeWhenArmed(workflowId, beforeStart);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Accepts every hint the claim loaded: the claim already left out the hint of a workflow that has an event after
     * the position the claim resumes from, see {@link RunningWorkflowsClaimLoader}.
     */
    @Override
    public ClaimedIndex withAcceptedHints(ClaimedIndex claimed) {
        return claimed;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Runs the restore under the lock of the workflow, which every hibernation, wake and adoption of it takes.
     */
    @Override
    public <T> T restoreGuarded(String workflowId, Supplier<T> restore) {
        try (var ignored = liveWorkflows.lock(workflowId)) {
            return restore.get();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Adopts each workflow on its own, see {@link #adopt(SegmentClaim, String, HibernationHint)}.
     */
    @Override
    public void adopt(SegmentClaim claim, Map<String, HibernationHint> hints) {
        if (hints.isEmpty()) {
            return;
        }
        var segment = claim.segment();
        int adopted = 0;
        for (var hinted : hints.entrySet()) {
            if (adopt(claim, hinted.getKey(), hinted.getValue())) {
                adopted++;
            } else {
                projection.recordWoken(segment, hinted.getKey());
            }
        }
        logger.debug("Left {} hibernated workflow(s) of segment {} out of memory.", adopted, segment.getSegmentId());
    }

    /**
     * Leaves the given workflow, whose hint a claim accepted, out of memory as a hibernated workflow of that claim,
     * instead of restoring it. Package-private so a test can adopt one workflow and see whether it was adopted.
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
        try (var ignored = liveWorkflows.lock(workflowId)) {
            if (!running || liveWorkflows.get(workflowId) != null) {
                return false;
            }
            var adopted = new AtomicBoolean();
            // Armed right away when bodies start now; else by the activation of the claim, see arm(Segment).
            claim.runIfCurrentWithBodyStart(bodyStart -> adopted.set(park(
                    hibernationOf(claim, workflowId, hint, bodyStart == BodyStart.NOW
                            ? CompletableFuture.completedFuture(null)
                            : new CompletableFuture<>()),
                    liveWorkflows::adopt
            )));
            return adopted.get();
        }
    }

    /**
     * Lets the workflows adopted by the claim of the given segment wake for their deadline or a cancellation, as the
     * claim caught up. A deadline that passed meanwhile wakes its workflow right away.
     *
     * @param segment the segment whose claim caught up
     */
    @Override
    public void arm(Segment segment) {
        liveWorkflows.hibernatedOwnedBy(segment)
                     .filter(hibernated -> hibernated.claim().segment().equals(segment))
                     .forEach(hibernated -> hibernated.armed().complete(null));
    }

    /**
     * Forgets the hibernated workflows of the given claim, whose restore failed. Leaves those of a later claim of the
     * same segment in place.
     *
     * @param claim the claim that failed
     */
    @Override
    public void dropOf(SegmentClaim claim) {
        drop(liveWorkflows.hibernatedOwnedBy(claim.segment())
                          .filter(hibernated -> hibernated.claim() == claim)
                          .toList());
    }

    /**
     * Forgets the hibernated workflows of the given segment, and of every segment overlapping it, which a split or a
     * merge replaced without a release. Their durable state is untouched: the next claim of their segment restores
     * them.
     *
     * @param segment the segment that is released or claimed
     */
    @Override
    public void dropOverlapping(Segment segment) {
        // Every hibernated workflow: one of a claim of an overlapping segment with a smaller mask may be owned by
        // another segment than this one.
        drop(liveWorkflows.hibernatedOwnedBy(null)
                          .filter(hibernated -> WorkflowSegmentOwnership.overlap(hibernated.claim().segment(), segment))
                          .toList());
    }

    /**
     * {@inheritDoc}
     * <p>
     * The restore wins, and the hibernation is forgotten like one of a released segment, see
     * {@link LiveWorkflows#restoreOver(Hibernated, WorkflowExecution, boolean)}.
     *
     * @throws IllegalStateException when the workflow is hibernated under the restoring claim itself, which restores
     *                               only the workflows it did not adopt
     */
    @Override
    public boolean restoreOverHibernation(SegmentClaim claim,
                                          String workflowId,
                                          WorkflowExecution execution,
                                          boolean mayHibernate) {
        if (!(liveWorkflows.get(workflowId) instanceof Hibernated hibernated)) {
            return false;
        }
        if (hibernated.claim() == claim) {
            throw new IllegalStateException(
                    "Workflow '%s' is hibernated under the claim that restores it.".formatted(workflowId)
            );
        }
        if (!liveWorkflows.restoreOver(hibernated, execution, mayHibernate)) {
            return false;
        }
        release(hibernated);
        logger.debug("Forgot hibernated workflow [{}] of segment {}, as a claim of segment {} restored it.",
                     workflowId, hibernated.claim().segment().getSegmentId(), claim.segment().getSegmentId());
        return true;
    }

    /**
     * Forgets the given hibernated workflows and cancels their deadlines. Takes the lock of each workflow, so a wake
     * that is loading finishes first.
     */
    private void drop(List<Hibernated> dropped) {
        dropped.forEach(hibernated -> {
            try (var ignored = liveWorkflows.lock(hibernated.workflowId())) {
                forget(hibernated);
            }
        });
    }

    /**
     * Removes the given hibernation, unless another one replaced it, with its wait rows and its deadline. The caller
     * holds the lock of the workflow.
     */
    private void forget(Hibernated hibernated) {
        if (liveWorkflows.drop(hibernated)) {
            release(hibernated);
        }
    }

    /**
     * Cancels the deadline of the given hibernation, which is no longer live. A wake held until the hibernation may
     * wake finds the workflow gone and does nothing, and a cancellation request that waited for it is made again, and
     * finds the workflow wherever it is now.
     */
    private void release(Hibernated hibernated) {
        hibernated.wakeTimer().cancel();
        hibernated.armed().complete(null);
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
        try (var ignored = liveWorkflows.lock(workflowId)) {
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
            var parked = new AtomicBoolean();
            claim.runIfCurrent(() -> {
                projection.recordHibernated(claim.segment(), workflowId, hint);
                parked.set(park(hibernationOf(claim, workflowId, hint, CompletableFuture.completedFuture(null)),
                                hibernated -> liveWorkflows.hibernate(execution, hibernated)));
            });
            if (parked.get()) {
                logger.debug("Workflow [{}] hibernated until an awaited event or its deadline at {}.",
                             workflowId, deadline);
            }
            return parked.get();
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
     * Returns a new hibernation of the given workflow under the given claim, with a timer for its nearest deadline,
     * which wakes it once the given {@code armed} future completed. Package-private so a test can build the hibernation
     * a workflow in memory hibernates into.
     *
     * @param claim      the claim the workflow hibernates under
     * @param workflowId the identifier of the workflow
     * @param hint       the open waits and nearest deadline it hibernates with
     * @param armed      completes once it may wake for its deadline or a cancellation
     * @return the hibernation, not yet live
     */
    Hibernated hibernationOf(SegmentClaim claim, String workflowId, HibernationHint hint,
                             CompletableFuture<Void> armed) {
        return new Hibernated(workflowId, claim, hint, liveWorkflows.rowsOf(workflowId, hint),
                              scheduler.schedule(hint.wakeAt()), armed);
    }

    /**
     * Keeps the workflow of the given hibernation out of memory until an awaited event, its deadline or a cancellation
     * wakes it, once the given transition made the hibernation live. The caller holds the lock of the workflow.
     *
     * @return {@code false} when the transition failed, in which case the timer of the hibernation is cancelled
     */
    private boolean park(Hibernated hibernated, Predicate<Hibernated> transition) {
        if (!transition.test(hibernated)) {
            hibernated.wakeTimer().cancel();
            return false;
        }
        hibernated.wakeTimer().completion().thenCompose(ignored -> wakeWhenArmed(hibernated.workflowId(), restored -> {
        }));
        return true;
    }

    /**
     * Restores a hibernated workflow from the event store and starts it. The caller holds the lock of the workflow.
     * <p>
     * The given action runs on the restored execution before its body starts, so the body cannot hibernate again
     * before it sees what woke it.
     */
    private void wake(Hibernated hibernated, Consumer<WorkflowExecution> beforeStart) {
        var workflowId = hibernated.workflowId();
        boolean loaded;
        try {
            loaded = loader.load(hibernated, beforeStart);
        } catch (RuntimeException e) {
            if (liveWorkflows.get(workflowId) != hibernated) {
                // Its claim ended while it woke, so it left this node.
                release(hibernated);
            }
            throw e;
        }
        if (loaded) {
            hibernated.wakeTimer().cancel();
            hibernated.armed().complete(null);
            projection.recordWoken(hibernated.claim().segment(), workflowId);
            logger.debug("Workflow [{}] woken from hibernation.", workflowId);
        } else {
            // It ended elsewhere, or its claim ended meanwhile.
            forget(hibernated);
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
        if (!(liveWorkflows.get(workflowId) instanceof Hibernated hibernated)) {
            return CompletableFuture.completedFuture(null);
        }
        return hibernated.armed().thenRunAsync(() -> wakeDetached(workflowId, beforeStart), wakeExecutor);
    }

    /**
     * Wakes a hibernated workflow on the calling wake thread, once fewer than
     * {@value #MAX_CONCURRENT_DETACHED_WAKES} such wakes load a workflow. A failed wake schedules its retry after the
     * idle delay.
     */
    private void wakeDetached(String workflowId, Consumer<WorkflowExecution> beforeStart) {
        if (!liveWorkflows.isHibernated(workflowId)) {
            // Woken or dropped meanwhile: no need to wait for a permit only to find that out under the lock.
            return;
        }
        detachedWakes.acquireUninterruptibly();
        try (var ignored = liveWorkflows.lock(workflowId)) {
            if (liveWorkflows.get(workflowId) instanceof Hibernated current) {
                wake(current, beforeStart);
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
        try (var ignored = liveWorkflows.lock(workflowId)) {
            if (!(liveWorkflows.get(workflowId) instanceof Hibernated current)) {
                return;
            }
            var retry = scheduler.schedule(clock.instant().plus(idleDelay));
            if (!liveWorkflows.replace(current, current.withWakeTimer(retry))) {
                retry.cancel();
                return;
            }
            current.wakeTimer().cancel();
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
         * Loads the workflow of the given hibernation from the event store into memory under the claim it hibernated
         * under, in its place, see {@link LiveWorkflows#wake(Hibernated, WorkflowExecution, Executor)}, and starts it
         * once the claim caught up, after handing the loaded execution to the given action. Fails when the workflow
         * cannot be loaded in time. When the action or the start fails, the loaded execution swaps back for the
         * hibernation while the claim is current, see {@link LiveWorkflows#hibernate(WorkflowExecution, Hibernated)},
         * and only leaves memory once the claim ended.
         *
         * @param hibernated  the hibernation of the workflow
         * @param beforeStart receives the loaded execution before its body starts
         * @return {@code false} when there was nothing to load: the workflow ended elsewhere, or the claim ended
         */
        boolean load(Hibernated hibernated, Consumer<WorkflowExecution> beforeStart);
    }
}
