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
package io.axoniq.framework.workflow.simulation.harness;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Durable event-store substrate for the simulator: an append-only {@link InMemoryEventStorageEngine} that
 * (a) records the committed workflow event log for invariant checking and (b) can be armed to lose the next commit,
 * reproducing the write-then-vanish (F-0) crash window.
 * <p>
 * This object is the <strong>durable</strong> part of the simulated world: it survives a simulated crash (engine
 * shutdown + restart) unchanged, so recovery rides on the same append-only log the engine wrote before the crash
 * (INVARIANTS.md INV-3). A fresh {@code EngineInstance} is started over this same engine after a crash.
 * <p>
 * <strong>Write-then-vanish:</strong> {@link #armVanishNextCommit()} makes the very next {@code commit()} run the
 * caller's transaction body but skip the underlying append, so the event never reaches the durable log — exactly the
 * effect of a process dying after its {@code execute} action ran but before its {@code COMPLETED} event committed
 * (ARCHITECTURE.md §7; the {@code ExecuteDelegate} {@code // FIXME -> consider to use QOS} window). The recorded
 * committed log and the underlying engine stay consistent because the recording happens only on a real commit.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ControllableEventStorageEngine implements EventStorageEngine {

    private final InMemoryEventStorageEngine delegate = new InMemoryEventStorageEngine();
    // Single source of truth for the committed log: every durably-committed event (workflow + warmup) in commit order.
    // The workflow-event view ({@link #committedWorkflowLog()}) is DERIVED from this list, and recovery rebuilds the
    // store from this SAME list ({@link #recoveredFrom}). Keeping one ordered list (rather than a parallel
    // workflow-only list) guarantees the order the invariants read (the before-crash snapshot) and the order recovery
    // replays from can never diverge — a previous parallel `committedLog` could, under the rapid concurrent commits of
    // the combinator workflow, append in a different relative order than `committedTagged`, so the INV-3
    // (CommittedHistorySurvivesCrash) before/after comparison spuriously flagged a within-instance "reorder" that was a
    // harness recording artifact, not an engine or durability defect (timing-dependent; never seed-determined). See
    // POC-TLA-DST.adoc finding F-2 (intra-instance global-append-order note) and RegressionSeedsTest (seed 370).
    private final List<TaggedEventMessage<?>> committedTagged = new CopyOnWriteArrayList<>();
    private final AtomicBoolean vanishNextCommit = new AtomicBoolean(false);
    private final AtomicBoolean duplicateNextCommit = new AtomicBoolean(false);
    @Nullable
    private volatile String vanishStepName;
    @Nullable
    private volatile StepStatus vanishStepStatus;
    @Nullable
    private volatile String duplicateStepName;
    @Nullable
    private volatile StepStatus duplicateStepStatus;
    private volatile EventMessage lastVanishedEvent;
    private final AtomicInteger foreignAppends = new AtomicInteger();
    // A commit that never answers (the store hangs): the append future is never completed, the transaction rolled
    // back, so nothing is durable and the caller's FutureResolver is what decides when to give up.
    private final AtomicBoolean stallNextCommit = new AtomicBoolean(false);
    @Nullable
    private volatile String stallStepName;
    @Nullable
    private volatile StepStatus stallStepStatus;
    private final AtomicInteger stalledCommits = new AtomicInteger();
    // A commit that fails outright (the store throws): the append future completes exceptionally with a plain
    // runtime exception that is neither a DCB rejection nor a timeout.
    private final AtomicBoolean failNextCommit = new AtomicBoolean(false);
    @Nullable
    private volatile String failStepName;
    @Nullable
    private volatile StepStatus failStepStatus;
    private final AtomicInteger failedCommits = new AtomicInteger();
    private final AtomicBoolean fenceNextCommit = new AtomicBoolean(false);
    @Nullable
    private volatile String fenceStepName;
    @Nullable
    private volatile StepStatus fenceStepStatus;
    @Nullable
    private volatile String fencedWorkflowId;

        @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                ProcessingContext processingContext,
                                                                List<TaggedEventMessage<?>> events) {
        return delegate.appendEvents(condition, processingContext, events)
                       .thenApply(transaction -> new RecordingAppendTransaction(transaction, events));
    }

        @Override
    public MessageStream<EventMessage> source(SourcingCondition condition,
                                              ProcessingContext processingContext) {
        return delegate.source(condition, processingContext);
    }

        @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        return delegate.stream(condition);
    }

        @Override
    public CompletableFuture<TrackingToken> firstToken() {
        return delegate.firstToken();
    }

        @Override
    public CompletableFuture<TrackingToken> latestToken() {
        return delegate.latestToken();
    }

        @Override
    public CompletableFuture<TrackingToken> tokenAt(java.time.Instant at) {
        return delegate.tokenAt(at);
    }

    /**
     * Phase 4 {@code DUPLICATED_APPEND}: arms the store to record the NEXT commit twice in the durable recovery log
     * (an at-least-once store — a retried append whose first attempt actually landed). The live delegate commit is
     * unchanged; the duplicate surfaces on the next crash+recovery replay via {@link #recoveredFrom(List)}.
     * Single-shot — disarms after one commit.
     */
    public void armDuplicateNextCommit() {
        duplicateStepName = null;
        duplicateStepStatus = null;
        duplicateNextCommit.set(true);
    }

    /**
     * Arms a <em>targeted</em> duplicate: the next commit carrying a workflow step event for {@code stepName} with
     * {@code status} is recorded TWICE in the durable recovery log, and only that one — the targeted-vanish pattern
     * ({@link #armVanishCommitFor}) applied to the {@code DUPLICATED_APPEND} fault, so a depth probe can duplicate a
     * specific record class (a {@code RETRYING} attempt, a {@code migrateVersion} marker — a COMPLETED step event
     * whose step name is the {@code changeId} — or a combinator-decision step) regardless of how many other commits
     * precede it. Single-shot — disarms after the matching commit.
     *
     * @param stepName the step whose transition commit to duplicate.
     * @param status   the step status of the transition to duplicate.
     */
    public void armDuplicateCommitFor(String stepName, StepStatus status) {
        duplicateStepName = stepName;
        duplicateStepStatus = status;
        duplicateNextCommit.set(true);
    }

    /**
     * Returns whether the duplicate-append fault is armed for the next commit.
     *
     * @return {@code true} when the next commit will be recorded twice in the durable log.
     */
    public boolean isDuplicateArmed() {
        return duplicateNextCommit.get();
    }

    /**
     * Arms the next {@code commit()} to vanish: it runs but does not append to the durable log. Single-shot — it
     * disarms itself after one commit (whether or not that commit carried workflow events).
     */
    /**
     * Arms a stall: the next commit carrying a step event of {@code stepName} with {@code status} rolls back and
     * returns a future that never completes. The store has hung; only the caller's resolution timeout ends the wait.
     * Single-shot, volatile (a crash rebuilds the store and drops the arm).
     *
     * @param stepName step whose event the stalled commit must carry.
     * @param status   status of that step event.
     */
    public void armStallCommitFor(String stepName, StepStatus status) {
        stallStepName = stepName;
        stallStepStatus = status;
        stallNextCommit.set(true);
    }

    /**
     * Whether a stall is still armed; {@code false} once a commit consumed it.
     *
     * @return {@code true} while armed.
     */
    public boolean isStallArmed() {
        return stallNextCommit.get();
    }

    /**
     * Commits that were stalled so far.
     *
     * @return the count.
     */
    public int stalledCommits() {
        return stalledCommits.get();
    }

    /**
     * Arms a failure: the next commit carrying a step event of {@code stepName} with {@code status} rolls back and
     * completes exceptionally with an {@link IllegalStateException}, neither a DCB rejection nor a timeout. Single-shot,
     * volatile.
     *
     * @param stepName step whose event the failed commit must carry.
     * @param status   status of that step event.
     */
    public void armFailCommitFor(String stepName, StepStatus status) {
        failStepName = stepName;
        failStepStatus = status;
        failNextCommit.set(true);
    }

    /**
     * Whether a failure is still armed; {@code false} once a commit consumed it.
     *
     * @return {@code true} while armed.
     */
    public boolean isFailArmed() {
        return failNextCommit.get();
    }

    /**
     * Commits that were failed so far.
     *
     * @return the count.
     */
    public int failedCommits() {
        return failedCommits.get();
    }

    public void armVanishNextCommit() {
        vanishStepName = null;
        vanishStepStatus = null;
        vanishNextCommit.set(true);
    }

    /**
     * Arms a <em>targeted</em> vanish: the next commit carrying a workflow step event for {@code stepName} with
     * {@code status} is dropped from the durable log, and only that one. This pins the write-then-vanish window to a
     * specific step transition (e.g. {@code chargePayment} {@code COMPLETED}) regardless of how many other commits
     * precede it, so the F-0 reproduction is deterministic: the effect has already run by the time the COMPLETED
     * commit is attempted, so dropping it leaves the step STARTED in the durable log with the effect counted.
     *
     * @param stepName the step whose transition commit to drop.
     * @param status   the step status of the transition to drop.
     */
    public void armVanishCommitFor(String stepName, StepStatus status) {
        vanishStepName = stepName;
        vanishStepStatus = status;
        vanishNextCommit.set(true);
    }

    /**
     * Returns {@code true} while a vanish is armed and has not yet been consumed by a commit.
     *
     * @return whether the next commit is set to vanish.
     */
    public boolean isVanishArmed() {
        return vanishNextCommit.get();
    }

    /**
     * Clears any armed vanish. The arm is volatile crash intent, not durable state, so a crash/recovery clears it —
     * this prevents a leftover arm from repeatedly dropping the same step's commit on the recovered engine.
     */
    public void disarmVanish() {
        vanishNextCommit.set(false);
        vanishStepName = null;
        vanishStepStatus = null;
    }

    /**
     * Appends a no-op warmup event directly to the durable log (not a workflow event). The simulator appends a couple
     * of these before any workflow so workflow start triggers land at a positive event-store index, which lets
     * recovery replay them after a reset (an event-store reset position is exclusive of itself).
     *
     * @param warmup the warmup payload object.
     */
    public void appendWarmup(Object warmup) {
        var message = new GenericEventMessage(new MessageType(warmup.getClass()), warmup);
        var tagged = new GenericTaggedEventMessage<>(
                message, Set.of(Tag.of("type", message.type().qualifiedName().toString())));
        delegate.appendEvents(AppendCondition.none(), null, List.of(tagged)).join().commit().join();
        committedTagged.add(tagged);
        // Warmups carry no workflowId, so the derived committedWorkflowLog() (workflow events only) filters them out.
    }

    /**
     * Returns the tagged events that have durably committed (warmups + workflow events), in order — the durable
     * substrate carried across a crash into a fresh engine on recovery.
     *
     * @return the committed tagged events.
     */
        public List<TaggedEventMessage<?>> committedTaggedEvents() {
        return List.copyOf(committedTagged);
    }

    /**
     * Creates a <em>fresh</em> {@link ControllableEventStorageEngine} pre-populated with the given durably-committed
     * tagged events, used on crash recovery.
     * <p>
     * The committed log is the durable substrate and survives a crash; building a brand-new engine instance from it
     * (exactly the events that durably committed, in order, with their original tags) gives the recovered engine's
     * processor a clean stream to replay from — the same shape {@code WorkflowReplayPreparedStateTest} relies on
     * (pre-populate an in-memory engine, then start the app so it replays). A brand-new instance (rather than swapping
     * an inner delegate) carries no shared streaming/coordinator state from the crashed engine. A vanished commit is
     * not in the committed tagged events, so it is correctly absent after recovery, preserving the write-then-vanish
     * (F-0) window.
     *
     * @param committed the durably-committed tagged events to pre-populate.
     * @return a fresh engine seeded with {@code committed}.
     */
        public static ControllableEventStorageEngine recoveredFrom(List<TaggedEventMessage<?>> committed) {
        var fresh = new ControllableEventStorageEngine();
        for (TaggedEventMessage<?> tagged : committed) {
            fresh.delegate.appendEvents(AppendCondition.none(), null, List.of(tagged)).join().commit().join();
            fresh.committedTagged.add(tagged);
        }
        return fresh;
    }

    /**
     * Returns the last workflow event whose commit was vanished, if any (for diagnostics / Phase-5 trace logging).
     *
     * @return the vanished event, or empty if none vanished yet.
     */
        public Optional<EventMessage> lastVanishedEvent() {
        return Optional.ofNullable(lastVanishedEvent);
    }

    /**
     * Returns an ordered, immutable snapshot of every workflow event that has durably committed to the log.
     * <p>
     * "Durably committed" means the underlying append actually happened (a vanished commit is not in here), which is
     * exactly the set INV-3 ({@code CommittedHistorySurvivesCrash}) and INV-4 ({@code DeterministicReplay}) reason
     * about. Only events carrying a {@code workflowId} are kept (warmup/non-workflow events are filtered out).
     *
     * @return the committed workflow event log, oldest first.
     */
        public List<EventMessage> committedWorkflowLog() {
        // Derived from the single committedTagged source (in commit order), filtered to workflow events. This is the
        // SAME ordered source recovery rebuilds from, so the before-crash snapshot and the post-recovery log can never
        // disagree on order for any instance's events (see the committedTagged field comment).
        return committedTagged.stream()
                              .<EventMessage>map(TaggedEventMessage::event)
                              .filter(e -> MetadataUtils.hasWorkflowId().test(e.metadata()))
                              .toList();
    }

    /**
     * Renders the committed workflow log as compact one-line-per-event strings for failure diagnostics.
     *
     * @return human-readable lines describing each committed workflow event in order.
     */
        public List<String> renderCommittedLog() {
        var lines = new ArrayList<String>();
        int index = 0;
        for (EventMessage event : committedWorkflowLog()) {
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            var status = MetadataUtils.getStepStatus(event.metadata()).map(Enum::name)
                                      .or(() -> MetadataUtils.getWorkflowStatus(event.metadata()).map(Enum::name))
                                      .orElse("-");
            var stepName = MetadataUtils.getStepStatus(event.metadata()).isPresent()
                    ? MetadataUtils.getStepName(event.metadata())
                    : "<workflow>";
            lines.add(String.format("[%02d] wf=%s step=%s status=%s type=%s",
                                     index++, workflowId, stepName, status,
                                     event.type().qualifiedName()));
        }
        return List.copyOf(lines);
    }

    /**
     * Arms a <em>deterministic fence</em>: right before the next commit carrying a workflow step event for
     * {@code stepName} with {@code status} runs, a foreign write for that instance
     * ({@link #appendForeign(String)}) lands, so the commit is rejected by its own append condition.
     * <p>
     * The window is the real one. The store checks the condition twice — once when the transaction is created and
     * again at commit — so writing in between is exactly the interleaving in which a peer wins a race the losing
     * writer had already passed the first check for. Single-shot: it disarms after the matching commit.
     *
     * @param stepName the step whose transition commit to fence.
     * @param status   the step status of the transition to fence.
     */
    public void armForeignWriteBeforeCommitOf(String stepName, StepStatus status) {
        fenceStepName = stepName;
        fenceStepStatus = status;
        fencedWorkflowId = null;
        fenceNextCommit.set(true);
    }

    /**
     * Arms the fence on the <em>next</em> commit that carries a workflow event, whichever instance and step that turns
     * out to be — the untargeted form of {@link #armForeignWriteBeforeCommitOf(String, StepStatus)}. It lands a
     * stale writer's event on whatever the workload happens to be doing at that moment rather than on one hand-picked
     * transition. Single-shot.
     */
    public void armForeignWriteBeforeNextCommit() {
        fenceStepName = null;
        fenceStepStatus = null;
        fencedWorkflowId = null;
        fenceNextCommit.set(true);
    }

    /**
     * Clears an armed fence that no commit consumed, so it cannot fire long after the fault that armed it.
     */
    public void disarmFence() {
        fenceNextCommit.set(false);
        fenceStepName = null;
        fenceStepStatus = null;
    }

    /**
     * Returns whether the armed fence has not been consumed by a commit yet.
     *
     * @return {@code true} while a fence is armed.
     */
    public boolean isFenceArmed() {
        return fenceNextCommit.get();
    }

    /**
     * Returns the instance the armed fence wrote for, once it fired — the landing evidence that the fence targeted a
     * real commit rather than never matching one.
     *
     * @return the fenced instance, or empty while the fence has not fired.
     */
        public Optional<String> fencedInstance() {
        return Optional.ofNullable(fencedWorkflowId);
    }

    /**
     * Appends one foreign event tagged {@code workflowId=<id>} straight to the durable log, under no append condition
     * and outside any workflow execution — the write a node that no longer owns the instance still lands, seen from
     * the store the new owner shares with it.
     * <p>
     * The event carries the instance's {@code workflowId} <em>tag</em> and no workflow metadata. The tag is what the
     * append condition of that instance is built from, so the very next append of whichever execution holds the
     * instance conflicts with it; the missing metadata keeps it out of {@link #committedWorkflowLog()}, so it
     * perturbs the fence and nothing else — no invariant reads it as a workflow record.
     *
     * @param workflowId the instance whose next append this write must fence.
     */
    public void appendForeign(String workflowId) {
        var payload = new ForeignWrite(workflowId, foreignAppends.incrementAndGet());
        var message = new GenericEventMessage(new MessageType(ForeignWrite.class), payload);
        var tagged = new GenericTaggedEventMessage<>(
                message, Set.of(Tag.of(WorkflowEventTags.TAG_WORKFLOW_ID, workflowId)));
        delegate.appendEvents(AppendCondition.none(), null, List.of(tagged)).join().commit().join();
        committedTagged.add(tagged);
    }

    /**
     * Returns how many foreign writes ({@link #appendForeign(String)}) the durable log holds — the landing evidence a
     * stale-writer fault actually perturbed the store. Counted from the durable log, so it survives a crash into the
     * engine {@link #recoveredFrom(List)} builds.
     *
     * @return the number of foreign writes in the durable log.
     */
    public int foreignWritesInLog() {
        return (int) committedTagged.stream()
                                    .filter(tagged -> tagged.event().payloadType() != null
                                            && ForeignWrite.class.getName()
                                                                 .equals(tagged.event().payloadType().getTypeName()))
                                    .count();
    }

    /**
     * Payload of a foreign write. Carries the instance it targets and a sequence number so two writes for one instance
     * are distinguishable in the durable log.
     *
     * @param workflowId the instance the write is tagged for.
     * @param sequence   ordinal of the write within the store instance that made it.
     */
    public record ForeignWrite(String workflowId, int sequence) {

    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("committedWorkflowEvents", committedWorkflowLog().size());
        descriptor.describeProperty("vanishArmed", vanishNextCommit.get());
    }

    /**
     * Append transaction that, on commit, either delegates (recording the events into the committed log) or — when a
     * vanish is armed — completes successfully without appending, dropping the events from the durable log.
     */
    private final class RecordingAppendTransaction implements AppendTransaction<Object> {

        @SuppressWarnings("unchecked")
        private final AppendTransaction<Object> delegateTransaction;
        private final List<TaggedEventMessage<?>> events;

        @SuppressWarnings("unchecked")
        private RecordingAppendTransaction(AppendTransaction<?> delegateTransaction,
                                           List<TaggedEventMessage<?>> events) {
            this.delegateTransaction = (AppendTransaction<Object>) delegateTransaction;
            this.events = events;
        }

                @Override
        public CompletableFuture<Object> commit() {
            if (vanishNextCommit.get() && matchesVanishTarget()) {
                vanishNextCommit.set(false);
                // Process "dies" before the commit lands: run nothing against the durable store, record nothing.
                delegateTransaction.rollback();
                events.stream()
                      .map(TaggedEventMessage::event)
                      .filter(e -> MetadataUtils.hasWorkflowId().test(e.metadata()))
                      .reduce((first, second) -> second)
                      .ifPresent(e -> lastVanishedEvent = e);
                return CompletableFuture.completedFuture(null);
            }
            if (stallNextCommit.get() && matchesTarget(stallStepName, stallStepStatus)) {
                stallNextCommit.set(false);
                stalledCommits.incrementAndGet();
                // The store hangs: nothing durable, and the caller never hears back.
                delegateTransaction.rollback();
                return new CompletableFuture<>();
            }
            if (failNextCommit.get() && matchesTarget(failStepName, failStepStatus)) {
                failNextCommit.set(false);
                failedCommits.incrementAndGet();
                delegateTransaction.rollback();
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Simulated event store failure on commit"));
            }
            if (fenceNextCommit.get() && matchesTarget(fenceStepName, fenceStepStatus)) {
                // A peer wins the race in the window between the condition check that created this transaction and
                // the one that commits it. Only a commit that carries a workflow event consumes the arm: a warmup or
                // a non-workflow append has no instance to write for.
                events.stream()
                      .map(TaggedEventMessage::event)
                      .filter(e -> MetadataUtils.hasWorkflowId().test(e.metadata()))
                      .findFirst()
                      .ifPresent(e -> {
                          fenceNextCommit.set(false);
                          var workflowId = MetadataUtils.getWorkflowId(e.metadata());
                          fencedWorkflowId = workflowId;
                          appendForeign(workflowId);
                      });
            }
            boolean duplicate = duplicateNextCommit.get()
                    && matchesTarget(duplicateStepName, duplicateStepStatus);
            if (duplicate) {
                duplicateNextCommit.set(false);
                duplicateStepName = null;
                duplicateStepStatus = null;
            }
            return delegateTransaction.commit()
                                      .thenApply(result -> {
                                          // Record into the single committedTagged source of truth, in commit order.
                                          events.forEach(committedTagged::add);
                                          if (duplicate) {
                                              // DUPLICATED_APPEND (Phase 4): the durable log holds the events TWICE
                                              // (at-least-once store); the dup surfaces on crash+recovery replay.
                                              events.forEach(committedTagged::add);
                                          }
                                          return result;
                                      });
        }

        private boolean matchesVanishTarget() {
            return matchesTarget(vanishStepName, vanishStepStatus);
        }

        private boolean matchesTarget(@Nullable String targetStep, @Nullable StepStatus targetStatus) {
            if (targetStep == null || targetStatus == null) {
                // Untargeted: match the next commit unconditionally.
                return true;
            }
            return events.stream()
                         .map(TaggedEventMessage::event)
                         .anyMatch(e -> MetadataUtils.hasWorkflowId().test(e.metadata())
                                 && MetadataUtils.getStepStatus(e.metadata())
                                                 .map(s -> s == targetStatus).orElse(false)
                                 && targetStep.equals(MetadataUtils.getStepStatus(e.metadata()).isPresent()
                                                              ? MetadataUtils.getStepName(e.metadata()) : null));
        }

        @Override
        public void rollback() {
            delegateTransaction.rollback();
        }

                @Override
        public CompletableFuture<org.axonframework.eventsourcing.eventstore.ConsistencyMarker> afterCommit(
                Object result) {
            return delegateTransaction.afterCommit(result);
        }
    }
}
