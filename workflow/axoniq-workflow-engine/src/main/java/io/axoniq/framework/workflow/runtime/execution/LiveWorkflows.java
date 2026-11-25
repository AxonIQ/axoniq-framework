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
import io.axoniq.framework.workflow.runtime.execution.CancellationRoute.Departure;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.lock.Lock;
import org.axonframework.common.lock.LockFactory;
import org.axonframework.common.lock.PessimisticLockFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The workflows of one workflow module that are live on this node, in memory or hibernated: the one place where the
 * residency of a workflow changes.
 * <p>
 * Each live workflow has one entry, keyed by its identifier: {@link InMemory} while its execution is in memory,
 * {@link Hibernated} while it is hibernated. Every transition is a compare-and-set on that entry: entering adds an
 * entry only when the workflow has none, and every other transition removes or replaces only the entry the caller
 * found. Only the winner of a transition applies its side effects, scoped to that entry, so an execution that left
 * memory never touches the next entry of its workflow. A workflow that stays live keeps its key: hibernating, waking,
 * and a restore that takes over a hibernation replace its entry in place.
 * <table>
 *     <caption>Transitions of an entry, and the locks they need</caption>
 *     <tr><th>Transition</th><th>Entry</th><th>The caller holds the lock of</th><th>It takes the lock of</th></tr>
 *     <tr><td>{@link #enter enter}: a workflow starts, or a claim restores it</td><td>none to {@link InMemory}</td>
 *     <td>its claim, if any, and its workflow, unless it is a restore without hibernation</td><td>the bin</td></tr>
 *     <tr><td>{@link #leave leave}: it ended, stopped, or failed to start</td><td>{@link InMemory} to none</td>
 *     <td>nothing</td><td>the bin</td></tr>
 *     <tr><td>{@link #adopt adopt}: a claim leaves it hibernated</td><td>none to {@link Hibernated}</td>
 *     <td>its workflow and its claim</td><td>the bin</td></tr>
 *     <tr><td>{@link #hibernate hibernate}</td><td>{@link InMemory} to {@link Hibernated}</td>
 *     <td>its workflow and its claim</td><td>the swap lock, then the bin</td></tr>
 *     <tr><td>{@link #wake wake}, {@link #restoreOver restoreOver}</td><td>{@link Hibernated} to {@link InMemory}</td>
 *     <td>its workflow and its claim</td><td>the swap lock, then the bin</td></tr>
 *     <tr><td>{@link #replace replace}: its timer changes</td><td>{@link Hibernated} to {@link Hibernated}</td>
 *     <td>its workflow</td><td>the bin</td></tr>
 *     <tr><td>{@link #drop drop}: its claim ended</td><td>{@link Hibernated} to none</td><td>its workflow</td>
 *     <td>the bin</td></tr>
 *     <tr><td>{@link #clear clear}: the engine shuts down</td><td>any to none</td><td>nothing</td>
 *     <td>the bins</td></tr>
 * </table>
 * <b>Locks.</b> Where several are held, they are taken in this order, never the other way:
 * <ol>
 *     <li>the lock of a workflow, see {@link #lock(String)}, which serializes the decisions about one workflow:
 *     starting, adopting, hibernating, waking and dropping it, delivering an event to it when it may hibernate or is
 *     hibernated, and routing a cancellation request to it;</li>
 *     <li>the lock of a {@link SegmentClaim claim}, so a workflow never enters memory, hibernates or wakes under a
 *     claim that ended;</li>
 *     <li>the write lock of the swap lock of the {@link BroadcastAddressees}, for every move between memory and
 *     hibernation, so the lookup of the addressees of a broadcast event misses no workflow that moves while it runs;
 *     under it, only the entries, the rows of the open waits and the repository are touched;</li>
 *     <li>the lock of a bin of the entries, under which only the factory of an entering execution and the repository
 *     run. As that lock is a monitor, a custom repository that blocks while it saves or removes an execution keeps the
 *     transitions of the other workflows of that bin waiting.</li>
 * </ol>
 * Leaving memory takes no lock of a workflow: it runs on threads that hold the lock of a claim, such as the cleanup of
 * a failed claim and the activation of a claim that removes the executions whose restored state is terminal.
 * Compare-and-remove on the entry makes that lock unnecessary.
 * <p>
 * <b>The repository.</b> The {@link WorkflowExecutionRepository} stays the store of the executions in memory, which a
 * user may replace, and which the parameter resolver of a workflow's state reads. Only this registry writes it, in step
 * with the entries: entering and waking save the execution, and leaving removes it, under the lock of the bin of the
 * entry; hibernating removes it under the swap lock, right after its entry was replaced. Every execution in the
 * repository therefore belongs to a live workflow, and a workflow that enters memory never finds the execution of a
 * workflow that is still leaving it.
 * <p>
 * <b>Reading the entries.</b> The {@link SimpleWorkflowManager} reads the entries without a lock. As the key of a live
 * workflow never leaves the entries, reading the entry of a workflow that stays live finds it whenever the read runs,
 * and visiting every entry finds it exactly once, see {@link #entries()}.
 * <p>
 * <b>What an entry carries.</b> An {@link InMemory} entry carries the {@link CancellationRoute} of its execution, whose
 * phase changes without changing the entry: as the engine starts the body, see
 * {@link #markCancellable(WorkflowExecution)}, as a woken execution fails to start, see
 * {@link #failStart(WorkflowExecution, Throwable)}, and as the execution leaves memory. The {@link SafePoints} of the
 * executions in memory, and the {@link BroadcastAddressees} with the rows of the open waits of the hibernated
 * workflows, are owned by this registry and kept in step with the entries. This registry never calls the
 * {@link Hibernation} of the engine: a delivery takes the wake for an event as a parameter, see
 * {@link #deliver(String, EventMessage, ProcessingContext, WakeForEvent)}.
 * <p>
 * The workflow module creates one per module, from its {@link WorkflowExecutionRepository}, and hands it to its
 * {@link WorkflowEngine}, its {@link WorkflowCancellationService} and its {@link SimpleWorkflowManager}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public final class LiveWorkflows {

    private final WorkflowExecutionRepository repository;
    private final SegmentBucketedMap<LiveWorkflow> live = new SegmentBucketedMap<>();
    private final SafePoints safePoints = new SafePoints(entry -> live.get(entry.workflowId()) == entry);
    private final BroadcastAddressees addressees;
    private final LockFactory locks = PessimisticLockFactory.usingDefaults();

    /**
     * Creates the registry of the live workflows of a workflow module, keeping the executions in memory in the given
     * {@code repository}.
     *
     * @param repository the store of the executions in memory, which only this registry writes
     */
    @Internal
    public LiveWorkflows(WorkflowExecutionRepository repository) {
        this.repository = Objects.requireNonNull(repository, "The WorkflowExecutionRepository must not be null.");
        this.addressees = new BroadcastAddressees(repository);
    }

    /**
     * Obtains the lock of the given workflow, which serializes the decisions about it, see the class documentation:
     * every transition of the workflow but leaving memory, a delivery of an event to it when it may hibernate or is
     * hibernated, and the routing of a cancellation request to it.
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
     * not enter memory. From then on, the execution reports its safe points, see {@link SafePoints}.
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
        Entering entering = transition(workflowId, current -> {
            if (current != null) {
                return new Transition<>(current, new Entering.Present(current));
            }
            var execution = factory.get();
            var candidate = new InMemory(execution, mayHibernate, CancellationRoute.toStarted(execution));
            if (repository.save(workflowId, candidate::execution) != execution) {
                return new Transition<>(null, new Entering.Conflict());
            }
            return new Transition<>(candidate, new Entering.Entered(candidate));
        });
        return switch (entering) {
            case Entering.Entered(var entered) -> {
                // Outside the lock of the bin: the listener reports the current safe point right away, and that report
                // checks that this entry is the workflow's entry.
                entered.execution().registerCheckpointWorkStateListener(safePoints.listenerOf(entered));
                yield entered.execution();
            }
            case Entering.Present(InMemory present) -> present.execution();
            // A guard: no caller gets here. A start checks under the lock of the workflow that it is not hibernated,
            // and a restore, under that lock as well, takes over a hibernation of the workflow instead of entering,
            // see restoreOver.
            case Entering.Present(Hibernated hibernated) ->
                    throw new IllegalStateException("Workflow '%s' is hibernated on this node.".formatted(workflowId));
            case Entering.Conflict() -> throw new IllegalStateException(
                    "Workflow '%s' already has an execution on this node.".formatted(workflowId)
            );
        };
    }

    /**
     * Takes the given execution out of memory, as it ended, stopped, or failed to start, and marks it safe: work it
     * still has in progress no longer holds back a checkpoint of its segment. Only removes the given execution, never
     * another one: an execution that left memory never removes the entry, the stored execution, or the safe point of
     * the workflow's next execution, nor the hibernation it moved into.
     * <p>
     * Aborts a workflow cancellation the execution did not perform yet, and answers the cancellation requests queued on
     * it, see {@link CancellationRoute#leave(CancellationRoute.Departure)}.
     *
     * @param execution the execution that leaves memory
     * @return {@code true} when the execution was in memory and left it, {@code false} when it was not in memory
     */
    boolean leave(WorkflowExecution execution) {
        var workflowId = execution.workflowId();
        // Removes the execution from the repository and its entry under the lock of the bin of the entry, so an enter
        // of the workflow waits until both are gone. The repository removal is scoped to the execution, and runs also
        // when the execution has no entry, so an execution in the repository is never stuck there.
        @Nullable InMemory entry = transition(workflowId, current -> {
            repository.remove(workflowId, execution);
            return current instanceof InMemory left && left.execution() == execution
                    ? new Transition<@Nullable InMemory>(null, left)
                    : new Transition<@Nullable InMemory>(current, null);
        });
        if (entry == null) {
            return false;
        }
        // After the entry, see SafePoints.
        safePoints.forget(entry);
        entry.cancellationRoute().leave(Departure.COMPLETED);
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
        addressees.add(hibernated);
        if (live.putIfAbsent(workflowId, hibernated) != null) {
            addressees.remove(workflowId);
            return false;
        }
        return true;
    }

    /**
     * Swaps the given execution in memory for the given hibernation: removes the execution from the repository and adds
     * the open waits of the hibernation to the rows, in one {@link BroadcastAddressees#swap(Supplier) swap}, and marks
     * the execution safe. The workflow hibernates, or an execution that
     * {@link #wake(Hibernated, WorkflowExecution, Executor) woke} from the hibernation and failed to start swaps back
     * for it. The caller holds the lock of the workflow, and the lock of the claim of the hibernation while that claim
     * is current, so a workflow never hibernates under a claim that ended.
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
        boolean hibernatedNow = addressees.swap(() -> {
            // Rows first, so the hibernated entry has them once it replaced the entry in memory.
            addressees.add(hibernated);
            if (!live.replace(workflowId, entry, hibernated)) {
                addressees.remove(workflowId);
                return false;
            }
            repository.remove(workflowId, execution);
            return true;
        });
        if (!hibernatedNow) {
            return false;
        }
        safePoints.forget(entry);
        entry.cancellationRoute().leave(Departure.HIBERNATED);
        return true;
    }

    /**
     * Swaps the given hibernation for the given execution, which the engine restored from the event store to wake the
     * workflow, see {@link #swapIn(Hibernated, InMemory)}. The caller holds the lock of the workflow and the lock of
     * the claim of the hibernation, and starts the execution once this returned. The execution may hibernate again.
     * <p>
     * Every cancellation request for the woken workflow is queued as a task of the woken execution. When the execution
     * leaves memory before its body took such a request, the request is answered on the given executor, never on the
     * thread on which the execution leaves memory, see {@link CancellationRoute}.
     *
     * @param hibernated the hibernation of the workflow
     * @param execution  the execution restored from the event store
     * @param answerOn   answers the requests queued on the woken execution when it leaves memory before its body took
     *                   them
     * @return {@code false} when the hibernation is no longer the workflow's entry, so the workflow did not wake
     * @throws IllegalStateException when the repository returns another execution of the workflow than the given one
     */
    boolean wake(Hibernated hibernated, WorkflowExecution execution, Executor answerOn) {
        return swapIn(hibernated, new InMemory(execution, true, CancellationRoute.toWoken(execution, answerOn)));
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
        return swapIn(hibernated, new InMemory(execution, mayHibernate, CancellationRoute.toStarted(execution)));
    }

    /**
     * Replaces the given hibernation with the given entry in memory, in place: saves its execution in the repository
     * and removes the rows of the workflow, in one {@link BroadcastAddressees#swap(Supplier) swap}. From then on, the
     * execution reports its safe points, see {@link SafePoints}.
     */
    private boolean swapIn(Hibernated hibernated, InMemory entry) {
        var workflowId = hibernated.workflowId();
        var execution = entry.execution();
        SwapIn swappingIn = addressees.swap(() -> {
            SwapIn swap = transition(workflowId, current -> {
                if (current != hibernated) {
                    return new Transition<>(current, SwapIn.REPLACED_MEANWHILE);
                }
                if (repository.save(workflowId, entry::execution) != execution) {
                    return new Transition<>(current, SwapIn.CONFLICT);
                }
                return new Transition<>(entry, SwapIn.SWAPPED);
            });
            if (swap == SwapIn.SWAPPED) {
                addressees.remove(workflowId);
            }
            return swap;
        });
        return switch (swappingIn) {
            case CONFLICT -> throw new IllegalStateException(
                    "Workflow '%s' already has an execution on this node.".formatted(workflowId)
            );
            case REPLACED_MEANWHILE -> false;
            case SWAPPED -> {
                execution.registerCheckpointWorkStateListener(safePoints.listenerOf(entry));
                yield true;
            }
        };
    }

    /**
     * Applies the given transition to the entry of the given workflow, under the lock of the bin of that entry, so no
     * other transition of the workflow interleaves, and returns its outcome. The transition gets the entry of the
     * workflow, or {@code null} when it has none, and returns the entry it leaves, with its outcome.
     */
    private <R> R transition(String workflowId, Function<@Nullable LiveWorkflow, Transition<R>> transition) {
        // The holder of the outcome, as a compute only returns the entry it leaves.
        var outcome = new AtomicReference<R>();
        live.compute(workflowId, (id, current) -> {
            var applied = transition.apply(current);
            outcome.set(applied.outcome());
            return applied.entry();
        });
        return outcome.get();
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
        addressees.remove(workflowId);
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
     * none that hibernates or wakes while it runs, see {@link BroadcastAddressees}. After the lookup, each addressee
     * gets the event under the lock of its workflow: one that hibernated since is woken only when its rows match the
     * event, one that woke gets it in memory, and one that left gets nothing. A failed delivery to one workflow does
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
        for (var workflowId : addressees.of(segment, event, context)) {
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
                    if (!whenAwaited || addressees.awaits(workflowId, event, context)) {
                        wake.wake(hibernated, event, context);
                    }
                    yield true;
                }
            };
        }
    }

    /**
     * Parses the open waits of the given hint into the rows of a hibernation of the given workflow, so a swap adds them
     * without parsing them under the swap lock, see {@link BroadcastAddressees#rowsOf(String, HibernationHint)}.
     *
     * @param workflowId the identifier of the workflow
     * @param hint       the open waits and nearest deadline the workflow hibernates with
     * @return the rows of the open waits
     */
    List<WaitAssociationIndex.Row> rowsOf(String workflowId, HibernationHint hint) {
        return addressees.rowsOf(workflowId, hint);
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
        return entryOf(execution).isPresent();
    }

    /**
     * Returns the entry of the given execution, while it is the execution in memory of its workflow.
     *
     * @param execution the execution to find the entry of
     * @return the entry of the execution, or empty when it left memory, or never entered
     */
    Optional<InMemory> entryOf(WorkflowExecution execution) {
        return live.get(execution.workflowId()) instanceof InMemory entry && entry.execution() == execution
                ? Optional.of(entry)
                : Optional.empty();
    }

    /**
     * Lists every execution in memory from the repository, not from the entries: a workflow that hibernates or wakes
     * while the repository lists its executions may be missed.
     *
     * @return an unmodifiable snapshot of the executions in memory
     */
    Set<WorkflowExecution> listInMemory() {
        return repository.findAll();
    }

    /**
     * Lists the executions in memory whose state matches the given query from the repository, see
     * {@link WorkflowExecutionRepository#findAll(WorkflowStateQuery)}, not from the entries: a workflow that hibernates
     * or wakes while the repository lists its executions may be missed.
     *
     * @param query the query the state of each returned execution matches
     * @return a future completing with the executions in memory matching the query
     */
    CompletableFuture<Set<WorkflowExecution>> listInMemory(WorkflowStateQuery query) {
        return repository.findAll(query);
    }

    /**
     * Lists the executions in memory owned by the given segment from the repository, which visits only the executions
     * of that segment, see {@link WorkflowExecutionRepository#findAllOwnedBy(Segment)}, not from the entries.
     *
     * @param segment the segment owning the executions, or {@code null} for every execution, as outside a segmented
     *                processor every execution is in scope
     * @return an unmodifiable snapshot of the executions in memory owned by the segment
     */
    Set<WorkflowExecution> listInMemoryOwnedBy(@Nullable Segment segment) {
        return repository.findAllOwnedBy(segment);
    }

    /**
     * Returns the safe points of the executions in memory: which of them have work in progress that makes a checkpoint
     * of their segment unsafe. Every execution reports them from the moment it enters memory until it leaves.
     *
     * @return the safe points of the executions in memory
     */
    SafePoints safePoints() {
        return safePoints;
    }

    /**
     * Lets cancellation requests for the given execution reach it from now on, as its body starts, unless it left
     * memory meanwhile: the <i>start</i> transition of its {@link CancellationRoute}.
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
        entryOf(execution).ifPresent(entry -> entry.cancellationRoute().start(provider.workflowCancellation()));
    }

    /**
     * Marks the given execution, which woke from hibernation, left, as its start failed, see
     * {@link CancellationRoute#failStart(Throwable)}: it takes no cancellation request from now on, and the requests
     * queued on it fail with the given failure. Its entry stays until the caller, which holds the lock of the workflow,
     * takes the execution out of memory next: it swaps it back for its hibernation, or lets it leave memory.
     *
     * @param execution the execution whose start failed
     * @param failure   the failure of the start
     */
    void failStart(WorkflowExecution execution, Throwable failure) {
        entryOf(execution).ifPresent(entry -> entry.cancellationRoute().failStart(failure));
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
        safePoints.clear();
        addressees.clear();
        inMemory.forEach(entry -> entry.cancellationRoute().leave(Departure.SHUT_DOWN));
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
     * leaves memory and enters it again, also by waking, gets a new entry, with a new {@link CancellationRoute}.
     */
    static final class InMemory implements LiveWorkflow {

        private final WorkflowExecution execution;
        private final boolean mayHibernate;
        private final CancellationRoute cancellationRoute;

        private InMemory(WorkflowExecution execution, boolean mayHibernate, CancellationRoute cancellationRoute) {
            this.execution = Objects.requireNonNull(execution, "The WorkflowExecution must not be null.");
            this.mayHibernate = mayHibernate;
            this.cancellationRoute = cancellationRoute;
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

        /**
         * Returns the route a cancellation request takes to the execution, which the
         * {@link WorkflowCancellationService} follows under the lock of the workflow.
         *
         * @return the cancellation route of the execution
         */
        CancellationRoute cancellationRoute() {
            return cancellationRoute;
        }

        @Override
        public String workflowId() {
            return execution.workflowId();
        }
    }

    /**
     * The entry of a hibernated workflow. Its timer and futures are its own, so two hibernations are equal only when
     * they are the same. It is data only: the engine's {@link Hibernation}, which created it, wakes it when the engine
     * or the {@link WorkflowCancellationService} asks it to.
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
     * What a transition makes of the entry of a workflow, see {@link #transition(String, Function)}.
     *
     * @param entry   the entry the transition leaves for the workflow, or {@code null} for none
     * @param outcome what the transition tells its caller
     * @param <R>     the type of the outcome
     */
    private record Transition<R>(@Nullable LiveWorkflow entry, R outcome) {

    }

    /**
     * The outcome of {@link #enter(String, boolean, Supplier) entering} memory.
     */
    private sealed interface Entering {

        /**
         * The execution of the given entry entered memory, and is saved in the repository.
         *
         * @param entry the entry of the execution that entered
         */
        record Entered(InMemory entry) implements Entering {

        }

        /**
         * The workflow was live already, with the given entry, so nothing entered.
         *
         * @param entry the entry of the workflow
         */
        record Present(LiveWorkflow entry) implements Entering {

        }

        /**
         * The repository holds another execution of the workflow, so nothing entered.
         */
        record Conflict() implements Entering {

        }
    }

    /**
     * The outcome of a swap of a hibernation for an execution in memory, see {@link #swapIn(Hibernated, InMemory)}.
     */
    private enum SwapIn {

        /**
         * The execution took the place of the hibernation.
         */
        SWAPPED,
        /**
         * Another entry replaced the hibernation meanwhile, so nothing changed.
         */
        REPLACED_MEANWHILE,
        /**
         * The repository holds another execution of the workflow, so nothing changed.
         */
        CONFLICT
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
}
