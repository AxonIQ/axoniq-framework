/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The workflows of one workflow module that are live in memory on this node, and the transitions that make a workflow
 * enter or leave memory.
 * <p>
 * Each workflow in memory has one entry, keyed by its identifier and holding its {@link WorkflowExecution}. Every
 * transition is a compare-and-set on that entry: entering adds it only when the workflow has none, and leaving removes
 * it only while it is the entry of the given execution. Only the winner applies the side effects, and those are scoped
 * to the entry as well, so an execution that left memory never touches the workflow's next execution, such as one
 * restored by a later claim of its segment or one woken from hibernation.
 * <p>
 * The {@link WorkflowExecutionRepository} stays the store of the executions in memory, which a user may replace, and
 * which the parameter resolver of a workflow's state reads. Only this class writes it, in step with the entries:
 * entering saves the execution, and leaving removes it, while it holds the lock of the bin of the workflow's entry, so
 * no other transition of that workflow interleaves. Every execution in the repository therefore has its entry here, and
 * a workflow that enters memory never finds the repository, or the entries, holding the execution of a workflow that is
 * still leaving it. Point lookups read the entries, while listings of executions read the repository, as a broadcast
 * event to the workflows of a segment does.
 * <p>
 * Under the lock of that bin, only the factory of the entering execution and the repository run. Neither touches this
 * registry, nor takes the lock of a workflow or of a claim, so the lock of the bin is always taken last. As the lock of
 * a bin is a monitor, a custom repository that blocks while it saves or removes an execution keeps the transitions of
 * the other workflows of that bin waiting, and keeps the carrier of a virtual thread that leaves memory.
 * <p>
 * The safe points of the workflows in memory are tracked here as well: the entries of the executions that have work in
 * progress that makes a checkpoint unsafe, see {@link #unsafeOf(Segment)}. An execution reports them through the
 * listener it gets when it enters memory, which only reports while the execution's entry is the workflow's entry: an
 * execution that left memory, for example because it hibernated or its segment was released, keeps unwinding for a
 * moment, and must not mark the workflow safe or unsafe on behalf of the next execution of that workflow.
 * <p>
 * Leaving memory takes no lock. It runs on threads that hold the lock of a claim, such as the cleanup of a failed claim
 * and the activation of a claim that removes the executions whose restored state is terminal, while the engine takes
 * the lock of a workflow before the lock of a claim, never the other way. Compare-and-remove on the entry makes such a
 * lock unnecessary.
 * <p>
 * The workflow module creates one per module, from its {@link WorkflowExecutionRepository}, and hands it to its
 * {@link WorkflowEngine}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public final class LiveWorkflows {

    private final WorkflowExecutionRepository repository;
    private final SegmentBucketedMap<InMemory> live = new SegmentBucketedMap<>();
    private final SegmentBucketedMap<InMemory> unsafe = new SegmentBucketedMap<>();

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
     * Brings the execution the given {@code factory} creates into memory as the execution of the given workflow, and
     * saves it in the repository: a workflow starts, is restored by a claim, or wakes from hibernation. When the
     * workflow is in memory already, or still leaving it, this waits until no other transition of the workflow is in
     * progress, and then returns the execution in memory without invoking the factory, or enters the workflow anew once
     * it left.
     * <p>
     * The execution enters only when the repository stores it: a repository whose
     * {@link WorkflowExecutionRepository#save(String, Supplier) save} returns another execution for the workflow, one
     * it holds without this registry having brought it into memory, fails the transition instead, and the workflow does
     * not enter memory. From then on, the execution reports its safe points to this registry, see
     * {@link #unsafeOf(Segment)}.
     *
     * @param workflowId the identifier of the workflow
     * @param factory    creates the execution of the workflow, which has the given identifier
     * @return the execution of the workflow in memory: the one the factory created, or the one in memory already
     * @throws IllegalStateException when the repository returns another execution of the workflow than the created one
     */
    WorkflowExecution enter(String workflowId, Supplier<WorkflowExecution> factory) {
        Objects.requireNonNull(factory, "The factory must not be null.");
        @Nullable InMemory[] created = new InMemory[1];
        boolean[] conflict = new boolean[1];
        // A compute rather than a computeIfAbsent, which may return a present entry without taking the lock of its
        // bin, and so the entry of an execution whose leave is in progress.
        var entry = live.compute(workflowId, (id, current) -> {
            if (current != null) {
                return current;
            }
            var candidate = new InMemory(factory.get());
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
        var entered = Objects.requireNonNull(entry, "An entry that entered memory without a conflict.");
        if (entered == created[0]) {
            // Outside the lock of the bin: the listener reports the current safe point right away, and that report
            // checks that this entry is the workflow's entry.
            entered.execution().registerCheckpointWorkStateListener(safePointsOf(entered));
        }
        return entered.execution();
    }

    /**
     * Takes the given execution out of memory, as it ended, stopped, hibernated, or failed to start, and marks it safe:
     * work it still has in progress no longer holds back a checkpoint of its segment. Only removes the given execution,
     * never another one: an execution that left memory never removes the entry, the stored execution, or the safe
     * point of the workflow's next execution.
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
            if (current != null && current.execution() == execution) {
                left[0] = current;
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
        return true;
    }

    /**
     * Returns the execution of the given workflow in memory.
     *
     * @param workflowId the identifier of the workflow
     * @return the execution in memory, or empty when the workflow is not in memory
     */
    Optional<WorkflowExecution> find(String workflowId) {
        return Optional.ofNullable(live.get(workflowId)).map(InMemory::execution);
    }

    /**
     * Returns whether the given execution is the one in memory for its workflow.
     *
     * @param execution the execution to check
     * @return {@code true} when the execution is in memory, {@code false} when it left, or never entered
     */
    boolean isInMemory(WorkflowExecution execution) {
        var entry = live.get(execution.workflowId());
        return entry != null && entry.execution() == execution;
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
     * Takes every execution out of memory, as the engine shuts down, and forgets their safe points. The caller stops
     * the executions first.
     */
    void clear() {
        repository.clear();
        live.clear();
        unsafe.clear();
    }

    /**
     * Returns the listener through which the execution of the given entry reports its safe points.
     * <p>
     * Marking unsafe checks, within the bucket of the unsafe entries, that the entry is still the entry of its
     * workflow. {@link #leave(WorkflowExecution) Leaving} removes the entry before it removes the unsafe mark, so a
     * mark either precedes that removal and is removed by it, or follows the removal of the entry and is refused: an
     * execution that left memory can never mark its successor's workflow unsafe. Marking safe only removes the entry's
     * own mark, so it never clears the mark of a successor.
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
     * The entry of a workflow whose execution is in memory. Its identity is that of one stay in memory: a workflow that
     * leaves memory and enters it again gets a new entry.
     */
    static final class InMemory {

        private final WorkflowExecution execution;

        private InMemory(WorkflowExecution execution) {
            this.execution = Objects.requireNonNull(execution, "The WorkflowExecution must not be null.");
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
         * Returns the identifier of the workflow.
         *
         * @return the workflow identifier
         */
        String workflowId() {
            return execution.workflowId();
        }
    }
}
