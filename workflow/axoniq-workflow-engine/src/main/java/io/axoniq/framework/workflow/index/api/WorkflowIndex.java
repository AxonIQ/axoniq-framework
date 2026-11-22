/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.api;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * The index of running workflow instances a workflow engine keeps, so that claiming a segment does not require reading
 * every workflow lifecycle event ever written.
 * <p>
 * Every workflow module runs its workflow instances in a workflow engine, which handles the workflow events of the
 * segments its event processor claims. When it claims a segment, the engine restores the workflow instances of that
 * segment that are still running. Without an index, finding those instances means reading every workflow lifecycle
 * event ever written. With an index, the engine loads the running instances of the segment through
 * {@link #load(String, Segment)}, and only reads the lifecycle events after the token its event processor resumes the
 * segment from. While it holds the segment, the engine writes the changes to the set of running instances through
 * {@link #store(String, IndexDelta, ProcessingContext)} whenever its event processor takes a checkpoint of the
 * segment.
 * <p>
 * Unless another index is configured, the workflow configuration uses {@link #none()}, which keeps nothing, so every
 * claim reads the running instances from the event store. A durable implementation shared by every node, such as the
 * {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex}, speeds up the claims after a restart and on other
 * nodes. Configure an implementation through
 * {@link io.axoniq.framework.workflow.configuration.WorkflowConfigurer#registerWorkflowIndex(
 *org.axonframework.common.configuration.ComponentBuilder)}:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .registerWorkflowIndex(config -> new MyWorkflowIndex(...));
 * }</pre>
 *
 * <h2>The index position is the token of the event processor</h2>
 * The index does not record a position of its own. The engine keeps every change that is not written yet in memory,
 * and writes all of them whenever its processor takes a checkpoint of the segment, before the processor stores the
 * token. So whenever the processor stores a token for a segment, every change that an event up to that token caused is
 * durable in the index. When a write fails, the checkpoint fails: the processor releases the segment and claims it
 * again after a back-off, and the claim restores the running instances from the event store as on any claim.
 * <p>
 * The engine hands every write of a checkpoint the context in which the processor stores the token. An index kept in
 * the same database as the tokens may write within the transaction of that context, so the changes and the token are
 * committed together, see {@link #store(String, IndexDelta, ProcessingContext)}. This only works while the processor
 * of the workflow module stores its tokens solely when the engine asks for it: with an index, a workflow module
 * refuses to start when its processor has ordinary event handling components, which do not checkpoint on their own,
 * or another way of storing its tokens.
 * <p>
 * On claim, the engine loads the entries of the segment and reads the lifecycle events from the token the segment
 * resumes from up to the head of the stream. The stored entries reflect at least every lifecycle event up to that
 * token, by the rule above, and the events from the token on complete them. Entries may also be <em>ahead</em> of the
 * token: changes written after the last stored checkpoint, for example right before a crash. That is harmless, as
 * whether an instance runs is decided by its last lifecycle event: re-applying the events from the token converges to
 * the same set of running instances. A crash between a write and the store of the token thus costs nothing but
 * re-applying those events.
 *
 * <h2>Initialization</h2>
 * Without a position, an index cannot tell from its entries alone whether they cover the stored tokens: an index that
 * was introduced while the processor already stored tokens, or that was emptied, holds none of the instances started
 * before those tokens. An index therefore reports whether it was {@link #initialize(String, Set) initialized}. As long
 * as it was not, {@link #load(String, Segment) load} reports nothing, and a claim reads the complete list of running
 * instances from the event store instead, and initializes the index with it. From then on, the rule above keeps the
 * entries covering every token the processor stores, provided that every node processing the workflow module writes
 * to the same index. An index that is not shared by those nodes, or that does not survive a restart while the tokens
 * do, must never report itself initialized: {@link #none()} never does.
 * <p>
 * Conversely, the tokens must survive a restart whenever the index does. A processor without stored tokens starts at
 * the head of the event stream, while the index is still initialized, so its claims would miss the instances whose
 * start the index did not store yet. A workflow module therefore refuses to start when it keeps an index other than
 * {@link #none()} or an {@link io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex} while its processor
 * keeps its tokens in an
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore}. For
 * the same reason, an application that deletes or resets the tokens of that processor must empty the index as well,
 * its initialization included.
 *
 * <h2>Contract for implementations</h2>
 * The index is derived data: the event store remains the source of truth. Losing the index, or emptying it together
 * with its initialization, only makes claims slower, as they read more lifecycle events from the event store. The
 * engine relies on the following:
 * <ul>
 *     <li><b>A write is atomic.</b> {@link #store(String, IndexDelta) store} and
 *     {@link #initialize(String, Set) initialize} either take effect as a whole or not at all, and a
 *     {@link #load(String, Segment) load} never sees part of one.</li>
 *     <li><b>Re-applying writes converges.</b> Adding an id that is stored, or removing one that is not, has no
 *     effect, so a write may be applied again.</li>
 *     <li><b>Writes may arrive out of order.</b> While it holds a segment, an engine issues the next write for that
 *     segment only once the previous one settled. Writes are not coordinated otherwise: writes of successive claims of
 *     a segment, on one node or on several, may interleave. A late write can only leave entries of instances that
 *     ended meanwhile, never drop a running one, as an instance never runs again once it ended; a later claim removes
 *     such entries once it finds the instance's state terminal.</li>
 *     <li><b>A future settles once the operation can no longer take effect.</b> A returned future completes once the
 *     operation took effect, and fails once it failed. A write must never take effect after its future reported it
 *     as failed or timed out: a durable implementation completes the future only once its transaction has ended,
 *     committed or rolled back. The engine relies on a completed write being durable when it lets the processor store
 *     a token past the changes of that write. A write within the transaction of a processing context is the exception:
 *     it is durable once that context commits, together with the token.</li>
 *     <li><b>Futures are bounded.</b> Implementations bound the time their futures take as far as the underlying
 *     resource allows, failing an operation that takes too long. The engine bounds the time a checkpoint waits for
 *     its writes as well, and fails the checkpoint when they take too long.</li>
 *     <li><b>Every valid workflow id is accepted.</b> The engine only starts workflow instances whose id is valid: at
 *     least one and at most {@value io.axoniq.framework.workflow.runtime.util.WorkflowIds#MAX_LENGTH} characters, all
 *     printable ASCII, see {@link io.axoniq.framework.workflow.runtime.util.WorkflowIds}. The index must hold the entry
 *     of every running instance with such an id. An implementation may reject a write that adds an invalid id, such as
 *     the id of an instance an earlier version of the engine started, by failing its future.</li>
 * </ul>
 * <p>
 * Every operation is scoped by an index name, unique per workflow module, so several workflow modules sharing one
 * index do not see each other's entries, and each is initialized on its own. Entries are keyed by workflow id, and a
 * load selects them through the segment's {@link Segment#matches(Object) ownership rule} applied to the workflow id, so
 * a segment split or merge needs no migration of entries.
 * <p>
 * Implementations must be thread-safe: the engine calls the index from several threads at once, for different
 * segments.
 * <p>
 * Marked {@link Internal} as a warning that API stability is not promised yet: this interface is usable, and supported
 * for configuring the workflow index and for custom implementations, but its shape may still change in a minor
 * release.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public interface WorkflowIndex {

    /**
     * Returns an index that keeps nothing and is never initialized, so that every claim reads the running workflow
     * instances of its segment from the event store.
     * <p>
     * The workflow engine keeps no index state at all with this index: it neither tracks nor writes the changes to the
     * running instances, and its checkpoints never wait for them, so it behaves as an engine without an index.
     * <p>
     * This is the default when no other index is configured, as an index kept by one node only cannot tell whether the
     * tokens stored by other nodes, or before a restart, passed changes it never saw.
     *
     * @return an index that keeps nothing
     */
    static WorkflowIndex none() {
        return NoWorkflowIndex.INSTANCE;
    }

    /**
     * Loads the running workflow instances owned by the given {@code segment}, or reports that the index was not
     * initialized.
     * <p>
     * The ids returned are those of the entries of the given index whose workflow id the segment
     * {@link Segment#matches(Object) matches}. An index that was not {@link #initialize(String, Set) initialized} under
     * the given name returns an empty {@link Optional}, whatever entries it holds.
     *
     * @param indexName the name of the index, unique per workflow module
     * @param segment   the segment to load the running instances of
     * @return a future completing with the ids of the running instances the segment owns, empty when the index was not
     * initialized, or failing when it cannot be loaded
     */
    CompletableFuture<Optional<Set<String>>> load(String indexName, Segment segment);

    /**
     * Applies the given {@code delta} to the index, in one atomic step.
     * <p>
     * Removing an id that is not stored, or adding one that is, has no effect, so a write may safely be applied again.
     * The returned future completes only once the delta took effect, and must not complete exceptionally while the
     * write may still take effect afterward.
     *
     * @param indexName the name of the index, unique per workflow module
     * @param delta     the changes to the set of running instances
     * @return a future completing once the write took effect, or failing once it can no longer take effect
     */
    CompletableFuture<Void> store(String indexName, IndexDelta delta);

    /**
     * Applies the given {@code delta} to the index, in one atomic step, as part of the checkpoint the processor stores
     * within the given {@code context}.
     * <p>
     * An implementation may write within the transaction of the {@code context}, on the calling thread, when the index
     * lives in the database that transaction belongs to. The changes and the token are then committed together, and the
     * write needs no database connection besides the one the processor already holds. The returned future then completes
     * once the delta is written within that transaction; the changes become durable only when the {@code context}
     * commits, and the engine keeps them pending again when it does not. An implementation that cannot join the
     * transaction writes as {@link #store(String, IndexDelta)} does.
     * <p>
     * Defaults to {@link #store(String, IndexDelta)}, ignoring the {@code context}.
     *
     * @param indexName the name of the index, unique per workflow module
     * @param delta     the changes to the set of running instances
     * @param context   the context the processor stores the checkpoint in
     * @return a future completing once the write took effect, or took effect within the transaction of the
     * {@code context}, or failing once it can no longer take effect
     */
    default CompletableFuture<Void> store(String indexName, IndexDelta delta, ProcessingContext context) {
        return store(indexName, delta);
    }

    /**
     * Adds the given running workflow instances to the index and marks it initialized, in one atomic step.
     * <p>
     * The engine calls this with every running instance of the workflow module, of all segments, as read from the
     * event store, when a claim found the index not initialized. It never removes an entry: another claim may have
     * written an instance that started after the given ids were read. Initializing an initialized index only adds the
     * given ids. The returned future completes only once the write took effect, and must not complete exceptionally
     * while the write may still take effect afterward.
     *
     * @param indexName          the name of the index, unique per workflow module
     * @param runningWorkflowIds the ids of every running instance of the workflow module
     * @return a future completing once the write took effect, or failing once it can no longer take effect
     */
    CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds);

    /**
     * A change to the set of running workflow instances.
     * <p>
     * An id is never both added and removed by one delta.
     * <p>
     * Marked {@link Internal} as a warning that API stability is not promised yet: this record is usable, and supported
     * for custom {@link WorkflowIndex} implementations, but its shape may still change in a minor release.
     *
     * @param added   the ids of the instances that started running
     * @param removed the ids of the instances that are no longer running
     */
    @Internal
    record IndexDelta(Set<String> added, Set<String> removed) {

        /**
         * Creates a delta, copying the given ids.
         *
         * @param added   the ids of the instances that started running
         * @param removed the ids of the instances that are no longer running
         * @throws IllegalArgumentException if an id is both added and removed
         */
        public IndexDelta {
            added = Set.copyOf(requireNonNull(added, "The added ids must not be null."));
            removed = Set.copyOf(requireNonNull(removed, "The removed ids must not be null."));
            for (String id : added) {
                if (removed.contains(id)) {
                    throw new IllegalArgumentException(
                            "The workflow id [" + id + "] cannot be both added and removed by one delta."
                    );
                }
            }
        }
    }
}
