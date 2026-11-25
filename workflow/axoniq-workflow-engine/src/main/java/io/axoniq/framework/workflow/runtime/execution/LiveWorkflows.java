/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.lock.Lock;
import org.axonframework.common.lock.LockFactory;
import org.axonframework.common.lock.PessimisticLockFactory;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The workflows of one workflow module that are live on this node, in memory or hibernated, and the transitions that
 * move a workflow into memory, out of it, and between memory and hibernation.
 * <p>
 * Each live workflow has one entry, keyed by its identifier: an {@link InMemory} entry holds its execution, a
 * {@link Hibernated} entry what wakes it again. Every transition is a compare-and-set on that entry: entering adds it
 * only when the workflow has none, leaving memory or this node removes it only while it is the entry of the given
 * execution or hibernation, and every transition of a workflow that stays live replaces it in place, only while it is
 * the given entry: hibernating, waking, and a restore that takes over a hibernation under another claim. So the key of
 * a live workflow never leaves the entries while the workflow stays live. Only the winner of a transition applies the
 * side effects, and those are scoped to the entry as well, so an execution that left memory never touches the
 * workflow's next entry, such as the execution restored by a later claim of its segment, or the hibernation it moved
 * into.
 * <p>
 * The {@link WorkflowExecutionRepository} stays the store of the executions in memory, which a user may replace, and
 * which the parameter resolver of a workflow's state reads. Only this class writes it, in step with the entries:
 * entering saves the execution, and leaving removes it, while it holds the lock of the bin of the workflow's entry, so
 * no other transition of that workflow interleaves. Every execution in the repository therefore belongs to a live
 * workflow, and a workflow that enters memory never finds the repository, or the entries, holding the execution of a
 * workflow that is still leaving it.
 * <p>
 * Under the lock of a bin, only the factory of the entering execution and the repository run. Neither touches this
 * registry, nor takes the lock of a workflow or of a claim, so the lock of a bin is always taken last. As the lock of a
 * bin is a monitor, a custom repository that blocks while it saves or removes an execution keeps the transitions of the
 * other workflows of that bin waiting, and keeps the carrier of a virtual thread that leaves memory.
 * <p>
 * <b>Hibernated workflows.</b> A hibernated workflow has no execution in memory. The open waits it hibernated with are
 * kept as rows of a {@link WaitAssociationIndex}, so a broadcast event wakes only the hibernated workflows that may
 * wait for it, without visiting the others. Hibernating moves a workflow from the repository to the rows, and waking or
 * a restore that takes over a hibernation moves it back, each in one swap. The rows of a hibernation are parsed when it
 * is created, see {@link #rowsOf(String, HibernationHint)}, so a swap only updates the entries, the rows and the
 * repository. The {@link WorkflowEngineHibernationSupport} decides when a workflow hibernates and wakes, and owns the
 * timers of the hibernated workflows; this registry applies the transitions, and never calls the support: a delivery
 * takes the wake of the engine's {@link Hibernation} as a parameter, see
 * {@link #deliver(String, EventMessage, ProcessingContext, WakeForEvent)}, and a cancellation request reports a
 * hibernated workflow back to the {@link WorkflowCancellationService}, see
 * {@link #request(String, Function, Supplier)}.
 * <p>
 * <b>Finding the addressees of a broadcast event.</b> A
 * {@link #broadcast(Segment, EventMessage, ProcessingContext, WakeForEvent) broadcast} event goes to every workflow of
 * the segment that is in memory, and to every hibernated workflow of the segment with an open wait that may match the
 * event. Its lookup lists the executions of the segment in the repository and matches the event against the rows.
 * Listing and matching at two moments could miss a workflow: one that is listed before it hibernates and matched once
 * it woke again is in neither result, and as its wake restores it from its own events, it would never see the broadcast
 * event. Every swap between memory and hibernation therefore runs under the write lock of a {@link StampedLock}, the
 * swap lock, and the lookup runs on an optimistic stamp of that lock. When a swap happened while it ran, the lookup
 * runs once more under the read lock of the swap lock, which no swap interleaves. The result is the state of the
 * repository and the rows at one instant between swaps, within the lookup.
 * <p>
 * The lookup relies on this: a swap moves a workflow between the repository and the rows only under the write lock,
 * and in one step. So at an instant between swaps, a workflow that stays live is found where it is: in the repository
 * while its execution is in memory, and by the rows of its open waits while it is hibernated. One that hibernated
 * without a wait for an event has no rows, and only an event of its own wakes it, which is delivered by its identifier,
 * not broadcast. The transitions that run without the swap lock only add a workflow to one side or remove it from one,
 * so a lookup may see them half done: inside {@link #leave(WorkflowExecution) leave}, the workflow is in neither while
 * its entry still stands, and an {@link #adopt(Hibernated) adoption} that finds the workflow entered meanwhile has its
 * rows for a moment while it is in memory, in which case the event still reaches it once. For these transitions,
 * "not live yet" or "no longer live" is a correct answer, as they are no moves of a workflow that stays live: a
 * workflow enters or leaves memory this way as it starts or ends, or as a claim of its segment starts or ends and
 * restores, adopts, stops or drops it, and a claim handles the events of its segment from the position it resumes from,
 * so an event routed while the claim changes is handled again by the next claim. So the lookup never misses a workflow
 * that hibernates, wakes, or hibernates and wakes while it runs: each such swap invalidates the stamp, and the repeated
 * lookup finds the workflow where it is. After the lookup, each addressee gets the event under the lock of its
 * workflow, see {@link #deliver(String, EventMessage, ProcessingContext, WakeForEvent)}: one that hibernated since is
 * woken only when its rows match the event, one that woke gets it in memory, and one that left gets nothing.
 * <p>
 * The cost of the lookup is one listing of the executions of the segment in memory, and one match of the rows, which
 * finds the keyed rows by the values the event carries and checks only those, the rows of the event's name without
 * key, and the rows without a name: hibernated workflows whose waits do not match the event are not visited. The swap
 * lock adds a volatile read and a validation of the stamp. Without hibernation, no swap ever runs, so the stamp never
 * fails. Swaps of all workflows of the module are serialized, which costs little, as a swap only updates the entries,
 * the rows and the repository.
 * <p>
 * A lookup never makes its first attempt under the read lock: a repository may, while it lists, wait for a workflow
 * to hibernate or wake on another thread, as the tests of this behavior do, and a read lock held by the listing
 * thread would keep that swap from ever finishing. When a swap holds the write lock as the lookup starts, the lookup
 * first waits for that swap to finish, and then makes its attempt on an optimistic stamp. Only a repeated attempt
 * holds the read lock.
 * <p>
 * <b>Finding the live workflows a query matches.</b> The {@link SimpleWorkflowManager} needs no swap lock: it reads the
 * entry of each workflow it looks for, or visits every entry once, and never matches the rows. As the key of a live
 * workflow never leaves the entries, reading the entry of a workflow that stays live finds it, in memory or
 * hibernated, whenever the read runs, and visiting every entry finds it exactly once, see {@link #entries()}. Only a
 * listing of the executions in memory, see {@link #inMemory(WorkflowStateQuery)}, misses a workflow that is
 * hibernated as the listing passes it; the manager documents how a query that lists them finds such a workflow.
 * <p>
 * <b>Locks.</b> The lock of a workflow, see {@link #lock(String)}, serializes the decisions about one workflow:
 * starting, adopting, hibernating, waking and dropping it, retrying its wake, delivering an event to it when it may
 * hibernate or is hibernated, and requesting its cancellation. Every writer of the rows of a workflow holds it, as the
 * rows are keyed by the workflow's identifier. Where the engine takes several locks, it takes the lock of a workflow,
 * then the lock of a {@link SegmentClaim claim}, then the write lock of the swap lock, then the lock of a bin of the
 * entries. The write lock of the swap lock is a leaf: under it, only the entries, the rows and the repository are
 * touched; nothing takes the lock of a workflow, of a claim or of the
 * {@link RunningWorkflowsProjection projection}, and nothing runs or loads a workflow. The read lock is held only by a
 * repeated lookup, which takes no other lock of this registry.
 * <p>
 * Leaving memory takes no lock. It runs on threads that hold the lock of a claim, such as the cleanup of a failed claim
 * and the activation of a claim that removes the executions whose restored state is terminal, while the engine takes
 * the lock of a workflow before the lock of a claim, never the other way. Compare-and-remove on the entry makes such a
 * lock unnecessary.
 * <p>
 * The safe points of the workflows in memory are tracked here as well: the entries of the executions that have work in
 * progress that makes a checkpoint unsafe, see {@link #unsafeOf(Segment)}. An execution reports them through the
 * listener it gets when it enters memory, which only reports while the execution's entry is the workflow's entry: an
 * execution that left memory, for example because it hibernated or its segment was released, keeps unwinding for a
 * moment, and must not mark the workflow safe or unsafe on behalf of the next execution of that workflow.
 * <p>
 * <b>Cancellation.</b> A cancellation request finds its workflow here, see
 * {@link #request(String, Function, Supplier)}, under the lock of the workflow, so it either reaches the execution
 * before it checks for a safe point, or finds the workflow hibernated. The {@link Phase phase} of an entry in memory
 * decides how a request reaches its execution: once the engine marks the execution
 * {@link #markCancellable(WorkflowExecution) cancellable}, as its body starts, a request is made right away; a request
 * for a hibernated workflow wakes it, and is queued as a task of the woken execution, whose body makes the request once
 * it takes the task, see {@link #queue(WorkflowExecution, QueuedRequest)}; so is every request for an execution that
 * woke from hibernation, as a woken body registers its running steps again only as it replays them. Each transition
 * that takes an execution out of memory answers the requests queued on it, so they are made again wherever the workflow
 * is then, and every one but hibernating aborts a workflow cancellation the execution did not perform yet; only when
 * the start of a woken execution fails do the requests queued on it fail with that failure, see
 * {@link #failStart(WorkflowExecution, Throwable)}.
 * <p>
 * The workflow module creates one per module, from its {@link WorkflowExecutionRepository}, and hands it to its
 * {@link WorkflowEngine} and its {@link WorkflowCancellationService}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public final class LiveWorkflows {

    private final WorkflowExecutionRepository repository;
    private final SegmentBucketedMap<LiveWorkflow> live = new SegmentBucketedMap<>();
    private final SegmentBucketedMap<InMemory> unsafe = new SegmentBucketedMap<>();
    private final WaitAssociationIndex waits = new WaitAssociationIndex();
    private final LockFactory locks = PessimisticLockFactory.usingDefaults();
    private final StampedLock swaps = new StampedLock();

    /**
     * Creates the registry of the live workflows of a workflow module, keeping the executions in memory in the given
     * {@code repository}.
     *
     * @param repository the store of the executions in memory, which only this registry writes
     */
    @Internal
    public LiveWorkflows(WorkflowExecutionRepository repository) {
        this.repository = Objects.requireNonNull(repository, "The WorkflowExecutionRepository must not be null.");
    }

    /**
     * Obtains the lock of the given workflow, which serializes the decisions about it, see the class documentation.
     *
     * @param workflowId the identifier of the workflow
     * @return the held lock, which the caller releases
     */
    Lock lock(String workflowId) {
        return locks.obtainLock(workflowId);
    }

    /**
     * Brings the execution the given {@code factory} creates into memory as the execution of the given workflow, and
     * saves it in the repository: a workflow starts or is restored by a claim. When the workflow is in memory already,
     * or still leaving it, this waits until no other transition of the workflow is in progress, and then returns the
     * execution in memory without invoking the factory, or enters the workflow anew once it left.
     * <p>
     * The execution enters only when the repository stores it: a repository whose
     * {@link WorkflowExecutionRepository#save(String, Supplier) save} returns another execution for the workflow, one
     * it holds without this registry having brought it into memory, fails the transition instead, and the workflow does
     * not enter memory. From then on, the execution reports its safe points to this registry, see
     * {@link #unsafeOf(Segment)}.
     *
     * @param workflowId   the identifier of the workflow
     * @param mayHibernate whether the execution may hibernate, so an event reaches it under the lock of its workflow,
     *                     see {@link #deliver(String, EventMessage, ProcessingContext, WakeForEvent)}
     * @param factory      creates the execution of the workflow, which has the given identifier
     * @return the execution of the workflow in memory: the one the factory created, or the one in memory already
     * @throws IllegalStateException when the repository returns another execution of the workflow than the created one,
     *                               or when the workflow is hibernated
     */
    WorkflowExecution enter(String workflowId, boolean mayHibernate, Supplier<WorkflowExecution> factory) {
        Objects.requireNonNull(factory, "The factory must not be null.");
        @Nullable InMemory[] created = new InMemory[1];
        boolean[] conflict = new boolean[1];
        // A compute rather than a computeIfAbsent, which may return a present entry without taking the lock of its
        // bin, and so the entry of an execution whose leave is in progress.
        var entry = live.compute(workflowId, (id, current) -> {
            if (current != null) {
                return current;
            }
            var candidate = new InMemory(factory.get(), mayHibernate, null);
            if (repository.save(id, candidate::execution) != candidate.execution()) {
                conflict[0] = true;
                return null;
            }
            created[0] = candidate;
            return candidate;
        });
        if (conflict[0]) {
            throw new IllegalStateException(
                    "Workflow '%s' already has an execution on this node.".formatted(workflowId)
            );
        }
        if (!(entry instanceof InMemory entered)) {
            // A guard: no caller gets here. A start checks under the lock of the workflow that it is not hibernated,
            // and a restore, under that lock as well, takes over a hibernation of the workflow instead of entering,
            // see restoreOver.
            throw new IllegalStateException("Workflow '%s' is hibernated on this node.".formatted(workflowId));
        }
        if (entered == created[0]) {
            // Outside the lock of the bin: the listener reports the current safe point right away, and that report
            // checks that this entry is the workflow's entry.
            entered.execution().registerCheckpointWorkStateListener(safePointsOf(entered));
        }
        return entered.execution();
    }

    /**
     * Takes the given execution out of memory, as it ended, stopped, or failed to start, and marks it safe: work it
     * still has in progress no longer holds back a checkpoint of its segment. Only removes the given execution, never
     * another one: an execution that left memory never removes the entry, the stored execution, or the safe point of
     * the workflow's next execution, nor the hibernation it moved into.
     * <p>
     * Aborts a workflow cancellation the execution did not perform yet, and answers the cancellation requests queued on
     * it, see {@link #queue(WorkflowExecution, QueuedRequest)}.
     *
     * @param execution the execution that leaves memory
     * @return {@code true} when the execution was in memory and left it, {@code false} when it was not in memory
     */
    boolean leave(WorkflowExecution execution) {
        var workflowId = execution.workflowId();
        @Nullable InMemory[] left = new InMemory[1];
        // Removes the execution from the repository and its entry under the lock of the bin of the entry, so an enter
        // of the workflow waits until both are gone. The repository removal is scoped to the execution, and runs also
        // when the execution has no entry, so an execution in the repository is never stuck there.
        live.compute(workflowId, (id, current) -> {
            repository.remove(id, execution);
            if (current instanceof InMemory entry && entry.execution() == execution) {
                left[0] = entry;
                return null;
            }
            return current;
        });
        var entry = left[0];
        if (entry == null) {
            return false;
        }
        // After the entry: a late report of the execution marking it unsafe then either preceded this removal, or
        // finds the entry gone and is refused, see safePointsOf.
        unsafe.remove(workflowId, entry);
        entry.leave(Departure.COMPLETED, null);
        return true;
    }

    /**
     * Leaves the given workflow, whose hint a claim accepted, out of memory as a hibernated workflow, instead of
     * restoring it. The caller holds the lock of the workflow and the lock of the claim, and schedules the timer of the
     * hibernation.
     *
     * @param hibernated the hibernation of the workflow
     * @return {@code false} when the workflow is live already, so it was not adopted
     */
    boolean adopt(Hibernated hibernated) {
        var workflowId = hibernated.workflowId();
        // Rows first, so a hibernated entry always has its rows. The caller checked under the lock of the workflow that
        // it is not live, and every transition that adds an entry holds that lock, so no entry can be there now; one
        // in memory would have no rows to lose here anyway.
        addWaits(hibernated);
        if (live.putIfAbsent(workflowId, hibernated) != null) {
            waits.removeWorkflow(workflowId);
            return false;
        }
        return true;
    }

    /**
     * Swaps the given execution in memory for the given hibernation: removes the execution from the repository and adds
     * the open waits of the hibernation to the rows, under the write lock of the swap lock, and marks the execution
     * safe. The workflow hibernates, or an execution that {@link #wake(Hibernated, WorkflowExecution) woke} from the
     * hibernation and failed to start swaps back for it. The caller holds the lock of the workflow, and the lock of
     * the claim of the hibernation while that claim is current, so a workflow never hibernates under a claim that
     * ended.
     * <p>
     * Answers the cancellation requests queued on the execution, so they are made again and reach the hibernated
     * workflow. Aborts no workflow cancellation: an execution that hibernates has none pending, as one keeps it from a
     * safe point, and before an execution whose start failed swaps back, the engine marks it left, which aborts its
     * pending one and fails the requests queued on it, see {@link #failStart(WorkflowExecution, Throwable)}.
     *
     * @param execution  the execution that hibernates
     * @param hibernated the hibernation the workflow moves into
     * @return {@code false} when the execution is no longer the one in memory, so the workflow did not hibernate
     */
    boolean hibernate(WorkflowExecution execution, Hibernated hibernated) {
        var workflowId = hibernated.workflowId();
        if (!(live.get(workflowId) instanceof InMemory entry) || entry.execution() != execution) {
            return false;
        }
        long stamp = swaps.writeLock();
        try {
            // Rows first, so the hibernated entry has them once it replaced the entry in memory.
            addWaits(hibernated);
            if (!live.replace(workflowId, entry, hibernated)) {
                waits.removeWorkflow(workflowId);
                return false;
            }
            repository.remove(workflowId, execution);
        } finally {
            swaps.unlockWrite(stamp);
        }
        unsafe.remove(workflowId, entry);
        entry.leave(Departure.HIBERNATED, null);
        return true;
    }

    /**
     * Swaps the given hibernation for the given execution, which the engine restored from the event store to wake the
     * workflow, see {@link #swapIn(Hibernated, InMemory)}. The caller holds the lock of the workflow and the lock of
     * the claim of the hibernation, and starts the execution once this returned. The execution may hibernate again.
     * <p>
     * Every cancellation request for the woken workflow is queued as a task of the woken execution, see
     * {@link #request(String, Function, Supplier)}. When the execution leaves memory before its body took such a
     * request, the request is answered on the given executor, never on the thread on which the execution leaves memory,
     * see {@link QueuedRequest}.
     *
     * @param hibernated the hibernation of the workflow
     * @param execution  the execution restored from the event store
     * @param answerOn   answers the requests queued on the woken execution when it leaves memory before its body took
     *                   them
     * @return {@code false} when the hibernation is no longer the workflow's entry, so the workflow did not wake
     * @throws IllegalStateException when the repository returns another execution of the workflow than the given one
     */
    boolean wake(Hibernated hibernated, WorkflowExecution execution, Executor answerOn) {
        return swapIn(hibernated, new InMemory(execution, true, answerOn));
    }

    /**
     * Swaps the given hibernation, under a claim of an overlapping segment, for the given execution, which a claim
     * restored from the event store, see {@link #swapIn(Hibernated, InMemory)}: the restore wins over the hibernation,
     * which is then forgotten like one of a released segment. Only while a split or a merge is in progress does a claim
     * of an overlapping segment hold a workflow hibernated that another claim restores. The caller holds the lock of
     * the workflow and the lock of the restoring claim, and cancels the timer of the hibernation.
     *
     * @param hibernated   the hibernation of the workflow under the other claim
     * @param execution    the execution the restoring claim restored
     * @param mayHibernate whether the execution may hibernate, see {@link #enter(String, boolean, Supplier)}
     * @return {@code false} when the hibernation is no longer the workflow's entry, so the execution did not enter
     * @throws IllegalStateException when the repository returns another execution of the workflow than the given one
     */
    boolean restoreOver(Hibernated hibernated, WorkflowExecution execution, boolean mayHibernate) {
        return swapIn(hibernated, new InMemory(execution, mayHibernate, null));
    }

    /**
     * Replaces the given hibernation with the given entry in memory, in place: saves its execution in the repository
     * and removes the rows of the workflow, under the write lock of the swap lock. From then on, the execution reports
     * its safe points to this registry.
     */
    private boolean swapIn(Hibernated hibernated, InMemory entry) {
        var workflowId = hibernated.workflowId();
        var execution = entry.execution();
        boolean[] swapped = new boolean[1];
        boolean[] conflict = new boolean[1];
        long stamp = swaps.writeLock();
        try {
            live.compute(workflowId, (id, current) -> {
                if (current != hibernated) {
                    return current;
                }
                if (repository.save(id, entry::execution) != execution) {
                    conflict[0] = true;
                    return current;
                }
                swapped[0] = true;
                return entry;
            });
            if (swapped[0]) {
                waits.removeWorkflow(workflowId);
            }
        } finally {
            swaps.unlockWrite(stamp);
        }
        if (conflict[0]) {
            throw new IllegalStateException(
                    "Workflow '%s' already has an execution on this node.".formatted(workflowId)
            );
        }
        if (!swapped[0]) {
            return false;
        }
        execution.registerCheckpointWorkStateListener(safePointsOf(entry));
        return true;
    }

    /**
     * Forgets the given hibernation, unless another entry replaced it, with its rows. The caller holds the lock of the
     * workflow, and cancels the timer of the hibernation.
     *
     * @param hibernated the hibernation to forget
     * @return {@code true} when the hibernation was the workflow's entry, and is removed
     */
    boolean drop(Hibernated hibernated) {
        var workflowId = hibernated.workflowId();
        if (!live.remove(workflowId, hibernated)) {
            return false;
        }
        waits.removeWorkflow(workflowId);
        return true;
    }

    /**
     * Replaces the given hibernation with the given one of the same workflow and with the same open waits, as the
     * timer that wakes it changes. The caller holds the lock of the workflow.
     *
     * @param current     the hibernation that must be the workflow's entry
     * @param replacement the hibernation to replace it with
     * @return {@code true} when the given hibernation was the workflow's entry, and is replaced
     */
    boolean replace(Hibernated current, Hibernated replacement) {
        if (!current.workflowId().equals(replacement.workflowId())) {
            throw new IllegalArgumentException("A hibernation can only be replaced by one of the same workflow.");
        }
        return live.replace(current.workflowId(), current, replacement);
    }

    /**
     * Delivers a broadcast event to the workflows of the given segment: to every execution in memory, and to the
     * hibernated workflows with an open wait that may match the event, which the given {@code wake} wakes, see
     * {@link #deliver(String, EventMessage, ProcessingContext, WakeForEvent)}. The lookup of these workflows misses
     * none that hibernates or wakes while it runs, see the class documentation. A failed delivery to one workflow does
     * not keep the event from the others: it is delivered to every workflow, after which the first failure is
     * rethrown.
     *
     * @param segment the segment handling the event, or {@code null} outside a segmented processor
     * @param event   the event to deliver
     * @param context the context of the event
     * @param wake    wakes a hibernated workflow that waits for the event, under the lock of the workflow
     */
    void broadcast(@Nullable Segment segment, EventMessage event, ProcessingContext context, WakeForEvent wake) {
        Objects.requireNonNull(wake, "The wake must not be null.");
        RuntimeException failure = null;
        for (var workflowId : addressees(segment, event, context)) {
            try {
                deliver(workflowId, event, context, wake, true);
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
     * workflow restored from the event store, which the given {@code wake} wakes on this thread.
     * <p>
     * An execution that may hibernate, and a hibernated workflow, get the event under the lock of the workflow, so the
     * event either reaches the execution before it checks for a safe point, or finds the workflow hibernated: without
     * the lock, the event could reach an execution that just hibernated, which drops it. An execution that never
     * hibernates gets the event without that lock.
     * <p>
     * This registry does not know how a workflow wakes: the engine hands it the wake of its {@link Hibernation}, which
     * it runs while it holds the lock of the workflow, so no other transition of the workflow interleaves between
     * finding it hibernated and waking it.
     *
     * @param workflowId the identifier of the workflow the event is for
     * @param event      the event to deliver
     * @param context    the context of the event
     * @param wake       wakes the workflow when it is hibernated, under the lock of the workflow
     * @return {@code false} when the workflow is not live here, so it belongs to another engine, which is expected in
     * multi-module setups
     */
    boolean deliver(String workflowId, EventMessage event, ProcessingContext context, WakeForEvent wake) {
        return deliver(workflowId, event, context, Objects.requireNonNull(wake, "The wake must not be null."), false);
    }

    private boolean deliver(String workflowId,
                            EventMessage event,
                            ProcessingContext context,
                            WakeForEvent wake,
                            boolean whenAwaited) {
        var entry = live.get(workflowId);
        if (entry == null) {
            return false;
        }
        if (entry instanceof InMemory inMemory && !inMemory.mayHibernate()) {
            inMemory.execution().onEvent(event, context);
            return true;
        }
        try (var ignored = lock(workflowId)) {
            return switch (live.get(workflowId)) {
                case null -> false;
                case InMemory inMemory -> {
                    inMemory.execution().onEvent(event, context);
                    yield true;
                }
                case Hibernated hibernated -> {
                    if (!whenAwaited || waits.matches(workflowId, event, context)) {
                        wake.wake(hibernated, event, context);
                    }
                    yield true;
                }
            };
        }
    }

    /**
     * Returns the workflows of the given segment a broadcast event goes to: every execution in memory, and every
     * hibernated workflow with an open wait that may match the event, as at one instant between swaps.
     */
    private Set<String> addressees(@Nullable Segment segment, EventMessage event, ProcessingContext context) {
        long stamp;
        while ((stamp = swaps.tryOptimisticRead()) == 0L) {
            // A swap is in progress: wait for it, but attempt the lookup without holding the read lock.
            swaps.unlockRead(swaps.readLock());
        }
        var addressees = lookUp(segment, event, context);
        if (swaps.validate(stamp)) {
            return addressees;
        }
        // A swap ran meanwhile: look up once more, while no swap runs.
        long readStamp = swaps.readLock();
        try {
            return lookUp(segment, event, context);
        } finally {
            swaps.unlockRead(readStamp);
        }
    }

    private Set<String> lookUp(@Nullable Segment segment, EventMessage event, ProcessingContext context) {
        var workflowIds = new LinkedHashSet<String>();
        repository.findAllOwnedBy(segment).forEach(execution -> workflowIds.add(execution.workflowId()));
        waits.match(event, context)
             .stream()
             .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
             .forEach(workflowIds::add);
        return workflowIds;
    }

    /**
     * Parses the open waits of the given hint into the rows of a hibernation of the given workflow, so a swap adds them
     * without parsing them under the swap lock.
     *
     * @param workflowId the identifier of the workflow
     * @param hint       the open waits and nearest deadline the workflow hibernates with
     * @return the rows of the open waits
     */
    List<WaitAssociationIndex.Row> rowsOf(String workflowId, HibernationHint hint) {
        return hint.waits().stream()
                   .map(wait -> waits.rowOf(
                           workflowId,
                           wait.stepName(),
                           wait.eventName() == null ? null : new QualifiedName(wait.eventName()),
                           wait.associations()
                   ))
                   .toList();
    }

    /**
     * Returns the entry of the given workflow.
     *
     * @param workflowId the identifier of the workflow
     * @return the entry, or {@code null} when the workflow is not live here
     */
    @Nullable
    LiveWorkflow get(String workflowId) {
        return live.get(workflowId);
    }

    /**
     * Returns whether the given workflow is hibernated here.
     *
     * @param workflowId the identifier of the workflow
     * @return {@code true} when the workflow's entry is a {@link Hibernated hibernation}
     */
    boolean isHibernated(String workflowId) {
        return live.get(workflowId) instanceof Hibernated;
    }

    /**
     * Returns the hibernated workflows owned by the given segment, visiting only the entries of that segment.
     *
     * @param segment the segment owning the workflows, or {@code null} for every hibernated workflow
     * @return a weakly consistent stream of the hibernations of the segment
     */
    Stream<Hibernated> hibernatedOwnedBy(@Nullable Segment segment) {
        return live.entriesOf(segment)
                   .map(Map.Entry::getValue)
                   .<Hibernated>mapMulti((entry, hibernated) -> {
                       if (entry instanceof Hibernated hibernation) {
                           hibernated.accept(hibernation);
                       }
                   });
    }

    /**
     * Returns the identifiers of the hibernated workflows, which visits every live workflow.
     *
     * @return an unmodifiable snapshot of the identifiers of the hibernated workflows
     */
    Set<String> hibernatedWorkflowIds() {
        return hibernatedOwnedBy(null).map(Hibernated::workflowId).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Returns the entry of every live workflow, in memory or hibernated. As the key of a live workflow never leaves the
     * entries, see the class documentation, the stream holds exactly one entry of each workflow that stays live while
     * the stream is consumed, whichever entry it has when it is visited.
     *
     * @return a weakly consistent stream of the entries of the live workflows
     */
    Stream<LiveWorkflow> entries() {
        return live.values();
    }

    /**
     * Returns the execution of the given workflow in memory.
     *
     * @param workflowId the identifier of the workflow
     * @return the execution in memory, or empty when the workflow is not in memory
     */
    Optional<WorkflowExecution> find(String workflowId) {
        return live.get(workflowId) instanceof InMemory entry ? Optional.of(entry.execution()) : Optional.empty();
    }

    /**
     * Returns whether the given execution is the one in memory for its workflow.
     *
     * @param execution the execution to check
     * @return {@code true} when the execution is in memory, {@code false} when it left, or never entered
     */
    boolean isInMemory(WorkflowExecution execution) {
        return live.get(execution.workflowId()) instanceof InMemory entry && entry.execution() == execution;
    }

    /**
     * Returns every execution in memory, read from the repository.
     *
     * @return an unmodifiable snapshot of the executions in memory
     */
    Set<WorkflowExecution> inMemory() {
        return repository.findAll();
    }

    /**
     * Returns the executions in memory whose state matches the given query, read from the repository, see
     * {@link WorkflowExecutionRepository#findAll(WorkflowStateQuery)}.
     *
     * @param query the query the state of each returned execution matches
     * @return a future completing with the executions in memory matching the query
     */
    CompletableFuture<Set<WorkflowExecution>> inMemory(WorkflowStateQuery query) {
        return repository.findAll(query);
    }

    /**
     * Returns the executions in memory owned by the given segment, read from the repository, which visits only the
     * executions of that segment, see {@link WorkflowExecutionRepository#findAllOwnedBy(Segment)}.
     *
     * @param segment the segment owning the executions, or {@code null} for every execution, as outside a segmented
     *                processor every execution is in scope
     * @return an unmodifiable snapshot of the executions in memory owned by the segment
     */
    Set<WorkflowExecution> inMemoryOwnedBy(@Nullable Segment segment) {
        return repository.findAllOwnedBy(segment);
    }

    /**
     * Returns the entries of the workflows of the given segment whose execution in memory last reported work in
     * progress that makes a checkpoint unsafe, without visiting those of other segments: a segment checks them on every
     * checkpoint, while this registry holds the workflows of every segment on this node.
     * <p>
     * A segment token only covers the workflows that segment owns, so executions of other segments must not hold it
     * back, and their entries must not be {@link #markSafe(InMemory) marked safe} on its behalf either. An entry that
     * is leaving memory is left out, as it no longer is the entry of its workflow.
     *
     * @param segment the segment owning the workflows
     * @return a snapshot of the entries of the segment's unsafe workflows in memory
     */
    List<InMemory> unsafeOf(Segment segment) {
        return unsafe.entriesOf(segment)
                     .map(Map.Entry::getValue)
                     .filter(entry -> live.get(entry.workflowId()) == entry)
                     .toList();
    }

    /**
     * Marks the given entry safe, as its execution turned out to have no work in progress that makes a checkpoint
     * unsafe, unless another entry of its workflow replaced it meanwhile.
     *
     * @param entry the entry to mark safe
     */
    void markSafe(InMemory entry) {
        unsafe.remove(entry.workflowId(), entry);
    }

    /**
     * Lets cancellation requests for the given execution reach it from now on, see the class documentation, unless it
     * left memory meanwhile.
     *
     * @param execution the execution in memory that takes cancellation requests from now on
     * @throws IllegalStateException when the execution cannot be cancelled, as it is no
     *                               {@link WorkflowCancellationProvider}
     */
    void markCancellable(WorkflowExecution execution) {
        if (!(execution instanceof WorkflowCancellationProvider provider)) {
            throw new IllegalStateException(
                    "Workflow execution %s does not implement WorkflowCancellationProvider, so it cannot be cancelled."
                            .formatted(execution.workflowId())
            );
        }
        if (live.get(execution.workflowId()) instanceof InMemory entry && entry.execution() == execution) {
            entry.start(provider.workflowCancellation());
        }
    }

    /**
     * Makes the given cancellation request of the given workflow, which it finds under the lock of the workflow, as far
     * as this registry can: the entry of the workflow, whether an entry in memory woke from hibernation, and its
     * {@link Phase phase}, decide how.
     * <ul>
     *     <li>An execution that woke from hibernation, whether its body started or not: the request is
     *     {@link Requested.Made made} by queueing it as a task of that execution, see
     *     {@link #queue(WorkflowExecution, QueuedRequest)}, which the body makes once it takes the task, as a woken
     *     body registers its running steps again only as it replays them, and takes a task only once it waits again
     *     where it hibernated.</li>
     *     <li>{@link Started}, an execution started or restored by a claim whose body started: the request is
     *     {@link Requested.Made made} against the cancellation of the execution, right away.</li>
     *     <li>{@link Hibernated}: the request is not made, and the workflow is
     *     {@link Requested.WorkflowHibernated reported hibernated}, once the lock is released. The caller has the
     *     engine's {@link Hibernation} wake the workflow for it, which queues the request likewise, see
     *     {@link Hibernation#requestWhileHibernated(String, Function, Supplier)}. That happens outside the lock:
     *     a wake for a deadline waits for a permit to load before it takes the lock of the workflow, so a wake executor
     *     that runs the wake on the requesting thread must find that lock free.</li>
     *     <li>{@link AwaitingStart}, an execution started or restored by a claim whose body did not start, as the
     *     claim did not catch up, or no entry: not at all.</li>
     * </ul>
     * <p>
     * As a wake of the workflow holds its lock while it loads the workflow from the event store, a request waits for
     * that load, up to the wake timeout.
     *
     * @param workflowId the identifier of the workflow
     * @param request    makes the request against the cancellation of the workflow's execution
     * @param again      makes the request again, wherever the workflow is then, when it was queued on an execution
     *                   that left memory before its body took it
     * @param <T>        the type of the outcome of the request
     * @return the request made, or the workflow reported hibernated
     * @throws NoSuchElementException when the workflow is not live here, or its execution takes no cancellation
     *                                requests yet, as it is restored by a claim that did not catch up
     */
    <T> Requested<T> request(String workflowId,
                             Function<WorkflowCancellation, CompletableFuture<T>> request,
                             Supplier<CompletableFuture<T>> again) {
        Objects.requireNonNull(request, "The request must not be null.");
        Objects.requireNonNull(again, "The request made again must not be null.");
        try (var ignored = lock(workflowId)) {
            return switch (live.get(workflowId)) {
                case null -> throw noRunningWorkflow(workflowId);
                case Hibernated hibernated -> new Requested.WorkflowHibernated<>();
                case InMemory entry -> new Requested.Made<>(switch (entry.phase()) {
                    // Not seen here: an entry is left while in the entries only during a failed start of a woken
                    // execution, which holds the lock of the workflow, see Phase.
                    case Left left -> throw noRunningWorkflow(workflowId);
                    // Woken: its body registers its running steps again only as it replays them, started or not.
                    case Phase phase when entry.answerOn() instanceof Executor answerOn ->
                            queueOn(entry, new QueuedRequest<>(request, again, answerOn));
                    case Started(var cancellation) -> entry.request(cancellation, request);
                    case AwaitingStart() -> throw noRunningWorkflow(workflowId);
                });
            };
        }
    }

    /**
     * Queues the given request on the given entry of an execution that woke from hibernation, or makes it again when
     * that execution left memory meanwhile, which takes no lock. The caller holds the lock of the workflow.
     */
    private <T> CompletableFuture<T> queueOn(InMemory entry, QueuedRequest<T> queued) {
        if (!queue(entry.execution(), queued)) {
            queued.makeAgain();
        }
        return queued.result();
    }

    private static NoSuchElementException noRunningWorkflow(String workflowId) {
        return new NoSuchElementException("No running workflow found with id '" + workflowId + "'");
    }

    /**
     * Queues the given request as a task of the given execution, whose body makes the request against the
     * cancellation of that execution once it takes the task: a restored body takes it only once it waits again where
     * it hibernated, as every earlier wait is answered from its recorded state. When the execution leaves memory before
     * its body took the task, the transition that takes it out answers the request instead, see
     * {@link QueuedRequest}. The caller holds the lock of the workflow. An execution that left memory takes no request,
     * also when it left as the caller decided to queue on it, as leaving memory takes no lock. When the execution takes
     * no task, for example because its task queue is full, the request fails with that failure instead, and nothing
     * stays queued.
     *
     * @param execution the execution to queue the request on
     * @param queued    the request to queue
     * @return {@code true} when the request is the execution's to answer: queued, or failed as the execution took no
     * task; {@code false} when the execution is not in memory, so the caller makes the request again
     */
    boolean queue(WorkflowExecution execution, QueuedRequest<?> queued) {
        if (live.get(execution.workflowId()) instanceof InMemory entry && entry.execution() == execution) {
            entry.queue(queued);
            return true;
        }
        return false;
    }

    /**
     * Queues the given request on the execution of the given workflow when it woke from hibernation, see
     * {@link #queue(WorkflowExecution, QueuedRequest)}, under the lock of the workflow: a request whose own wake found
     * the workflow woken by another one, such as an event, is made once the woken body took it, as the steps of that
     * body are only registered once it replayed them.
     *
     * @param workflowId the identifier of the workflow
     * @param queued     the request to queue
     * @return {@code true} when the request is queued, {@code false} when the workflow has no execution in memory that
     * woke from hibernation, so the caller makes the request again
     */
    boolean queueOnWoken(String workflowId, QueuedRequest<?> queued) {
        try (var ignored = lock(workflowId)) {
            if (live.get(workflowId) instanceof InMemory entry && entry.queuesRequests()) {
                entry.queue(queued);
                return true;
            }
            return false;
        }
    }

    /**
     * Marks the given execution, which woke from hibernation, left, as its start failed: it takes no cancellation
     * request from now on, the requests queued on it fail with the given failure, and a workflow cancellation it did
     * not perform yet is aborted, also one its body was making as the start failed, see {@link Phase}. The caller
     * holds the lock of the workflow, and takes the execution out of memory next: it swaps it back for its hibernation,
     * or lets it leave memory.
     *
     * @param execution the execution whose start failed
     * @param failure   the failure of the start
     */
    void failStart(WorkflowExecution execution, Throwable failure) {
        if (live.get(execution.workflowId()) instanceof InMemory entry && entry.execution() == execution) {
            entry.leave(Departure.FAILED_TO_START, failure);
        }
    }

    /**
     * Returns the identifiers of the live workflows a cancellation can be requested for, see
     * {@link #request(String, Function, Supplier)}.
     *
     * @return a snapshot of the identifiers
     */
    List<String> cancellableWorkflowIds() {
        return live.entriesOf(null)
                   .filter(entry -> !(entry.getValue() instanceof InMemory inMemory) || inMemory.takesRequests())
                   .map(Map.Entry::getKey)
                   .toList();
    }

    /**
     * Takes every workflow out of the registry, as the engine shuts down, and forgets the safe points and the rows.
     * Aborts the workflow cancellations the executions did not perform yet, and answers the requests queued on them.
     * The caller stops the executions and drops the hibernated workflows first.
     */
    void clear() {
        var inMemory = live.entriesOf(null)
                           .map(Map.Entry::getValue)
                           .<InMemory>mapMulti((entry, executions) -> {
                               if (entry instanceof InMemory execution) {
                                   executions.accept(execution);
                               }
                           })
                           .toList();
        repository.clear();
        live.clear();
        unsafe.clear();
        waits.clear();
        inMemory.forEach(entry -> entry.leave(Departure.SHUT_DOWN, null));
    }

    private void addWaits(Hibernated hibernated) {
        hibernated.rows().forEach(waits::add);
    }

    /**
     * Returns the listener through which the execution of the given entry reports its safe points.
     * <p>
     * Marking unsafe checks, within the bucket of the unsafe entries, that the entry is still the entry of its
     * workflow. {@link #leave(WorkflowExecution) Leaving} and {@link #hibernate(WorkflowExecution, Hibernated)
     * hibernating} remove the entry before they remove the unsafe mark, so a mark either precedes that removal and is
     * removed by it, or follows the removal of the entry and is refused: an execution that left memory can never mark
     * its successor's workflow unsafe. Marking safe only removes the entry's own mark, so it never clears the mark of a
     * successor.
     */
    private CheckpointWorkStateListener safePointsOf(InMemory entry) {
        var workflowId = entry.workflowId();
        return new CheckpointWorkStateListener() {
            @Override
            public void onMarkedUnsafe() {
                unsafe.compute(workflowId, (id, current) -> live.get(id) == entry ? entry : current);
            }

            @Override
            public void onMarkedSafe() {
                unsafe.remove(workflowId, entry);
            }
        };
    }

    /**
     * The entry of a live workflow: {@link InMemory} while its execution is in memory, {@link Hibernated} while it is
     * hibernated.
     */
    sealed interface LiveWorkflow permits InMemory, Hibernated {

        /**
         * Returns the identifier of the workflow.
         *
         * @return the workflow identifier
         */
        String workflowId();
    }

    /**
     * The entry of a workflow whose execution is in memory. Its identity is that of one stay in memory: a workflow that
     * leaves memory and enters it again, also by waking, gets a new entry.
     * <p>
     * Whether its execution woke from hibernation, and its {@link Phase phase}, tell whether a cancellation request
     * reaches the execution, see {@link LiveWorkflows#request(String, Function, Supplier)}, and it holds the requests
     * queued on an execution that woke from hibernation, see {@link #queue(QueuedRequest)}.
     */
    static final class InMemory implements LiveWorkflow {

        private final WorkflowExecution execution;
        private final boolean mayHibernate;
        // Non-null when the execution woke from hibernation: fixed for the stay in memory, unlike the phase.
        private final @Nullable Executor answerOn;
        private final AtomicReference<Phase> phase = new AtomicReference<>(new AwaitingStart());
        private final Set<QueuedRequest<?>> queuedRequests = ConcurrentHashMap.newKeySet();

        /**
         * Creates the entry of the given execution, awaiting its start.
         *
         * @param answerOn answers the requests queued on an execution that woke from hibernation when it leaves memory
         *                 before its body took them, or {@code null} when the execution did not wake from hibernation
         */
        private InMemory(WorkflowExecution execution, boolean mayHibernate, @Nullable Executor answerOn) {
            this.execution = Objects.requireNonNull(execution, "The WorkflowExecution must not be null.");
            this.mayHibernate = mayHibernate;
            this.answerOn = answerOn;
        }

        /**
         * Returns the current phase of this entry.
         */
        private Phase phase() {
            return phase.get();
        }

        /**
         * Returns the executor that answers the requests queued on the execution when it leaves memory before its body
         * took them, which only an execution that woke from hibernation has.
         *
         * @return the executor, or {@code null} when the execution did not wake from hibernation
         */
        private @Nullable Executor answerOn() {
            return answerOn;
        }

        /**
         * The <i>start</i> transition, see {@link Phase}: the body of the execution starts, so requests reach it
         * through the given cancellation from now on. Changes nothing once the execution started or left.
         */
        private void start(WorkflowCancellation cancellation) {
            phase.updateAndGet(current -> current instanceof AwaitingStart ? new Started(cancellation) : current);
        }

        /**
         * The <i>leave</i> transition, see {@link Phase}: the execution leaves memory the given way, or failed to start
         * and leaves next, in which case the given failure fails the requests queued on it. Only the first departure
         * counts: the winner aborts a workflow cancellation the execution did not perform yet, unless it hibernated,
         * and answers the queued requests.
         */
        private void leave(Departure departure, @Nullable Throwable failure) {
            var left = new Left(departure, failure);
            switch (phase.getAndUpdate(current -> current instanceof Left ? current : left)) {
                case Left earlier -> {
                    // Only the first departure counts, and it answered the queued requests already.
                    return;
                }
                case Started(var cancellation) -> abortPendingWorkflowCancellation(cancellation, left);
                case AwaitingStart() -> {
                    // Its body never started, so nothing can be pending.
                }
            }
            answerQueued(left);
        }

        /**
         * Makes the given request against the given cancellation of the execution. A leave takes no lock: it changes
         * the phase to {@link Left} and then aborts a pending workflow cancellation, while this request makes one
         * pending and then checks whether the entry left, so one of the two aborts a request that the execution never
         * performs.
         */
        private <T> CompletableFuture<T> request(WorkflowCancellation cancellation,
                                                 Function<WorkflowCancellation, CompletableFuture<T>> request) {
            var result = request.apply(cancellation);
            if (phase() instanceof Left left) {
                abortPendingWorkflowCancellation(cancellation, left);
            }
            return result;
        }

        /**
         * Queues the given request as a task of the execution, and answers it right away when the execution left
         * memory meanwhile: a leave changes the phase to {@link Left} and then answers the queued requests, while this
         * adds the request and then checks whether the entry left. Each queued request is either taken by the body or
         * answered, once: whichever removes it from the queued requests first. When the execution takes no task, for
         * example because its task queue is full, nothing stays queued: the request fails with the reason it could not
         * be queued.
         */
        private void queue(QueuedRequest<?> queued) {
            queuedRequests.add(queued);
            try {
                execution.appendTask(taking -> {
                    if (queuedRequests.remove(queued)) {
                        queued.makeOn(this);
                    }
                });
            } catch (RuntimeException e) {
                if (queuedRequests.remove(queued)) {
                    queued.answer(e);
                }
                return;
            }
            if (phase() instanceof Left left) {
                answerQueued(left);
            }
        }

        /**
         * Answers each queued request once, as the given departure says: fails it with the failure of a start that
         * failed, or makes it again.
         */
        private void answerQueued(Left left) {
            queuedRequests.forEach(queued -> {
                if (queuedRequests.remove(queued)) {
                    queued.answer(left.failure());
                }
            });
        }

        /**
         * Returns whether a cancellation request reaches the execution, directly or queued as a task, see
         * {@link LiveWorkflows#request(String, Function, Supplier)}.
         */
        private boolean takesRequests() {
            return switch (phase()) {
                case Left left -> false;
                case Started started -> true;
                // Queued as a task of an execution that woke from hibernation until its body starts.
                case AwaitingStart() -> answerOn != null;
            };
        }

        /**
         * Returns whether a cancellation request is queued as a task of the execution, as it woke from hibernation,
         * see {@link LiveWorkflows#request(String, Function, Supplier)}.
         */
        private boolean queuesRequests() {
            return answerOn != null && !(phase() instanceof Left);
        }

        /**
         * Aborts a workflow cancellation pending on the given cancellation, as the execution left memory the given way,
         * unless it hibernated, which it only does without one.
         */
        private static void abortPendingWorkflowCancellation(WorkflowCancellation cancellation, Left left) {
            var abortion = left.abortion();
            if (abortion != null && cancellation instanceof WorkflowCancellation.Request request) {
                request.abortPendingWorkflowCancellation(abortion);
            }
        }

        /**
         * Returns the execution of the workflow in memory.
         *
         * @return the execution
         */
        WorkflowExecution execution() {
            return execution;
        }

        /**
         * Returns whether the execution may hibernate, as hibernation is enabled and it runs under a claim with an
         * execution context.
         *
         * @return {@code true} when the execution may hibernate
         */
        boolean mayHibernate() {
            return mayHibernate;
        }

        @Override
        public String workflowId() {
            return execution.workflowId();
        }
    }

    /**
     * The phase of an {@link InMemory} entry: whether its body started, and whether it left memory. With whether the
     * execution woke from hibernation, which is fixed for its stay in memory, it decides how a cancellation request
     * reaches the execution, see {@link LiveWorkflows#request(String, Function, Supplier)}.
     * <p>
     * Every entry starts {@link AwaitingStart awaiting its start}. Each transition is a compare-and-set on the phase, so
     * no transition needs a lock of its own:
     * <table>
     *     <caption>Phase transitions</caption>
     *     <tr><th>Phase</th><th><i>start</i></th><th><i>leave</i></th></tr>
     *     <tr><td>{@link AwaitingStart}</td><td>{@link Started}</td><td>{@link Left}</td></tr>
     *     <tr><td>{@link Started}</td><td>unchanged</td><td>{@link Left}</td></tr>
     *     <tr><td>{@link Left}</td><td>unchanged</td><td>unchanged: the first departure counts</td></tr>
     * </table>
     * <ul>
     *     <li><i>start</i>, see {@link LiveWorkflows#markCancellable(WorkflowExecution)}: the engine starts the body of
     *     the execution, under the lock of the claim of the execution, if any. A woken execution whose claim caught up
     *     starts as it wakes, so under the lock of its workflow as well; one that woke while its claim caught up starts
     *     with the restored executions of that claim, under the lock of the claim only.</li>
     *     <li><i>leave</i>: the execution left memory, as it ended, stopped or failed to start
     *     ({@link LiveWorkflows#leave(WorkflowExecution)}, no lock), hibernated or swapped back for its hibernation
     *     ({@link LiveWorkflows#hibernate(WorkflowExecution, Hibernated)}, the lock of the workflow), or the engine
     *     shut down ({@link LiveWorkflows#clear()}, no lock); or the start of a woken execution failed, which leaves
     *     memory next ({@link LiveWorkflows#failStart(WorkflowExecution, Throwable)}, the lock of the workflow). Every
     *     transition but the last removes the entry from the entries before it changes the phase, so an entry in the
     *     entries is {@link Left} only while the engine handles the failed start of a woken execution under the lock
     *     of its workflow, which every request takes.</li>
     * </ul>
     * As leaving takes no lock, a request that reached a started execution checks the phase once more after it was
     * made, see {@link InMemory#request(WorkflowCancellation, Function)}, and a request queued on a woken execution
     * checks it once it is queued, see {@link InMemory#queue(QueuedRequest)}.
     */
    private sealed interface Phase {

    }

    /**
     * An execution whose body did not start: a new or restored one takes no cancellation requests yet, and its body
     * starts once the claim of its segment caught up; one that woke from hibernation gets its requests queued as its
     * tasks.
     */
    private record AwaitingStart() implements Phase {

    }

    /**
     * An execution whose body started: a request is made against its cancellation right away, unless the execution
     * woke from hibernation, which still gets its requests queued as its tasks, as a woken body registers its running
     * steps again only as it replays them.
     *
     * @param cancellation the cancellation of the execution
     */
    private record Started(WorkflowCancellation cancellation) implements Phase {

    }

    /**
     * An execution that left memory, or whose start failed so it leaves next: it takes no requests.
     *
     * @param departure how it left, which tells whether a pending workflow cancellation is aborted
     * @param failure   the failure of a start that failed, which fails the requests queued on it, or {@code null}
     *                  when they are made again, wherever the workflow is then
     */
    private record Left(Departure departure, @Nullable Throwable failure) implements Phase {

        private Left {
            if ((departure == Departure.FAILED_TO_START) != (failure != null)) {
                throw new IllegalArgumentException("Only an execution that failed to start leaves with a failure.");
            }
        }

        /**
         * Returns the reason a workflow cancellation the execution did not perform yet is aborted with, carrying the
         * failure of a start that failed, or {@code null} when the execution hibernated, which aborts none.
         */
        private @Nullable CancellationException abortion() {
            if (departure.abortReason == null) {
                return null;
            }
            var abortion = new CancellationException(departure.abortReason);
            if (failure != null) {
                abortion.initCause(failure);
            }
            return abortion;
        }
    }

    /**
     * The entry of a hibernated workflow. Its timer and futures are its own, so two hibernations are equal only when
     * they are the same. It is data only: the engine's {@link Hibernation}, which created it, wakes it when the engine
     * or the {@link WorkflowCancellationService} hands it over.
     *
     * @param workflowId the identifier of the workflow
     * @param claim      the claim of the segment that owns it, which it wakes under
     * @param hint       the open waits and nearest deadline it hibernated with
     * @param rows       the rows of its open waits, parsed from the hint, see
     *                   {@link #rowsOf(String, HibernationHint)}
     * @param wakeTimer  the timer that wakes it at its nearest deadline
     * @param armed      completes once it may wake for its deadline or a cancellation
     */
    record Hibernated(String workflowId,
                      SegmentClaim claim,
                      HibernationHint hint,
                      List<WaitAssociationIndex.Row> rows,
                      WorkflowScheduler.ScheduledTask wakeTimer,
                      CompletableFuture<Void> armed) implements LiveWorkflow {

        /**
         * Returns this hibernation with the given timer.
         *
         * @param newWakeTimer the timer that wakes the workflow instead
         * @return the hibernation with the given timer
         */
        Hibernated withWakeTimer(WorkflowScheduler.ScheduledTask newWakeTimer) {
            return new Hibernated(workflowId, claim, hint, rows, newWakeTimer, armed);
        }
    }

    /**
     * Wakes a hibernated workflow for an event, on the thread delivering the event. The engine hands the wake of its
     * {@link Hibernation} to {@link #deliver(String, EventMessage, ProcessingContext, WakeForEvent) deliver} and
     * {@link #broadcast(Segment, EventMessage, ProcessingContext, WakeForEvent) broadcast}, which call it under the
     * lock of the workflow, once they found the workflow hibernated.
     */
    @FunctionalInterface
    interface WakeForEvent {

        /**
         * Wakes the workflow of the given hibernation, and hands it the given event before its body starts. The caller
         * holds the lock of the workflow.
         *
         * @param hibernated the hibernation of the workflow the event is for
         * @param event      the event that wakes the workflow
         * @param context    the context of the event
         */
        void wake(Hibernated hibernated, EventMessage event, ProcessingContext context);
    }

    /**
     * How far a cancellation request got in this registry, see {@link #request(String, Function, Supplier)}: it was
     * made, against an execution in memory or queued on it, or the workflow is hibernated, and only the engine's
     * {@link Hibernation} can make it.
     *
     * @param <T> the type of the outcome of the request
     */
    sealed interface Requested<T> {

        /**
         * The request was made against the execution in memory, or queued on it.
         *
         * @param outcome the outcome of the request
         * @param <T>     the type of the outcome of the request
         */
        record Made<T>(CompletableFuture<T> outcome) implements Requested<T> {

        }

        /**
         * The workflow is hibernated: the caller has the engine's {@link Hibernation} wake it for the request, now that
         * the lock of the workflow is released, see
         * {@link Hibernation#requestWhileHibernated(String, Function, Supplier)}.
         *
         * @param <T> the type of the outcome of the request
         */
        record WorkflowHibernated<T>() implements Requested<T> {

        }
    }

    /**
     * How an execution left memory, and why a workflow cancellation it did not perform yet can no longer be performed.
     */
    private enum Departure {

        /**
         * It hibernated, which it only does without a pending workflow cancellation, or it woke and swapped back for
         * its hibernation.
         */
        HIBERNATED(null),
        /**
         * It ended or stopped.
         */
        COMPLETED("Workflow execution completed before cancellation was performed"),
        /**
         * Its start failed: a woken execution, which then swaps back for its hibernation or leaves memory. The
         * {@link Left} phase carries the failure.
         */
        FAILED_TO_START("Workflow execution failed to start before cancellation was performed"),
        /**
         * The engine shut down.
         */
        SHUT_DOWN("Workflow engine shut down before cancellation was performed");

        private final @Nullable String abortReason;

        Departure(@Nullable String abortReason) {
            this.abortReason = abortReason;
        }
    }

    /**
     * A cancellation request queued as a task of an execution that woke from hibernation, see
     * {@link #queue(WorkflowExecution, QueuedRequest)}. The body of the execution makes the request once it takes the
     * task, on the thread that drives the execution, which also decides whether the execution hibernates, so the
     * request reaches the execution before it checks for a safe point. When the execution leaves memory before its body
     * took the task, the request is answered instead, on the given executor: made again through the cancellation
     * service, wherever the workflow is then, or, when the start of the execution failed, failed with that failure.
     * <p>
     * The executor answers the request, never the thread on which the execution leaves memory: that thread may hold
     * the lock of a claim, and a request made again takes the lock of its workflow, which the engine otherwise takes
     * before the lock of a claim. Only when the executor no longer accepts tasks, on shutdown, is the request answered
     * on the calling thread.
     *
     * @param <T> the type of the outcome of the request
     */
    static final class QueuedRequest<T> {

        private final Function<WorkflowCancellation, CompletableFuture<T>> request;
        private final Supplier<CompletableFuture<T>> again;
        private final Executor answerOn;
        private final CompletableFuture<T> result = new CompletableFuture<>();

        /**
         * Creates a request to queue.
         *
         * @param request  makes the request against the cancellation of an execution
         * @param again    makes the request again, through the cancellation service, wherever the workflow is then
         * @param answerOn answers the request when the execution leaves memory before its body took it
         */
        QueuedRequest(Function<WorkflowCancellation, CompletableFuture<T>> request,
                      Supplier<CompletableFuture<T>> again,
                      Executor answerOn) {
            this.request = Objects.requireNonNull(request, "The request must not be null.");
            this.again = Objects.requireNonNull(again, "The request made again must not be null.");
            this.answerOn = Objects.requireNonNull(answerOn, "The executor must not be null.");
        }

        /**
         * Returns the outcome of the request.
         *
         * @return a future completing with the outcome of the request, wherever it is made
         */
        CompletableFuture<T> result() {
            return result;
        }

        /**
         * Makes the request again, through the cancellation service, on the calling thread.
         */
        void makeAgain() {
            completeWith(again);
        }

        /**
         * Fails the request with the given failure.
         *
         * @param failure the failure of the request
         */
        void fail(Throwable failure) {
            result.completeExceptionally(failure);
        }

        /**
         * Makes the request against the cancellation of the execution of the given entry, whose body took it. A body
         * only runs once its execution is cancellable, so this only answers the request instead when the execution
         * left memory meanwhile, which takes no lock, as that departure says.
         */
        private void makeOn(InMemory entry) {
            switch (entry.phase()) {
                case Started(var cancellation) -> completeWith(() -> entry.request(cancellation, request));
                case Left left -> answer(left.failure());
                // Not seen: the engine marks an execution started before its body runs.
                case AwaitingStart() -> answer(null);
            }
        }

        /**
         * Answers the request on its executor, or on the calling thread once that executor no longer accepts tasks:
         * fails it with the given failure, or makes it again without one.
         */
        private void answer(@Nullable Throwable failure) {
            Runnable answer = () -> {
                if (failure == null) {
                    makeAgain();
                } else {
                    fail(failure);
                }
            };
            try {
                answerOn.execute(answer);
            } catch (RejectedExecutionException e) {
                answer.run();
            }
        }

        private void completeWith(Supplier<CompletableFuture<T>> outcome) {
            try {
                outcome.get().whenComplete((value, failure) -> {
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(failure);
                    }
                });
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }
    }
}
