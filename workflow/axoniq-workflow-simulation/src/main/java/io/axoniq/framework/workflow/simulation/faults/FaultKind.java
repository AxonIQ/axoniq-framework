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
package io.axoniq.framework.workflow.simulation.faults;

/**
 * The catalogue of faults the simulator can inject, each annotated with the invariant it most stresses (per
 * INVARIANTS.md). The harness draws a {@code FaultKind} from the seeded RNG each step (or {@link #NONE}).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public enum FaultKind {

    /**
     * No fault this step — let the engine make progress.
     */
    NONE,

    /**
     * Worker crash mid-step: drop volatile engine state (live executions, processor token, in-flight timers), keep the
     * committed event log, then recover/restart over the same durable substrate. Stresses
     * {@code CommittedHistorySurvivesCrash} (INV-3) and {@code DeterministicReplay} (INV-4).
     */
    WORKER_CRASH,

    /**
     * Message reorder / delay / duplicate: reorder, delay, or duplicate the delivered external events. Stresses
     * {@code AtMostOnceRecording} (INV-2, dedup under redelivery) and {@code DeterministicReplay} (INV-4).
     */
    MESSAGE_REORDER,

    /**
     * Clean restart: stop the engine and recover from the event store with no other perturbation. Stresses
     * {@code CommittedHistorySurvivesCrash} (INV-3) and {@code EventuallyTerminates} (INV-5, resume after restart).
     */
    RESTART,

    /**
     * Clock jump: advance the mutable clock + virtual-time scheduler by a large delta, firing pending wait
     * timeouts/retry backoff. Stresses {@code EventuallyTerminates} (INV-5) and the {@code orTimeout} timeout math.
     */
    CLOCK_JUMP,

    /**
     * Write-then-vanish: crash in the window after an {@code execute} action ran but before its {@code COMPLETED}
     * event commits (the F-0 window). Stresses {@code EffectAtMostOnce} (INV-6) — the effect re-runs on replay.
     */
    WRITE_THEN_VANISH,

    /**
     * Duplicate-completed: re-deliver the external <em>wakeup</em> ({@code PaymentConfirmedEvent}) of an order instance
     * that is already <em>terminal</em> in the committed log — the after-the-fact duplicate delivery a flaky transport
     * produces. Where {@link #MESSAGE_REORDER}'s duplicate mode redelivers a still-<em>pending</em> trigger, this
     * redelivers the wakeup of an already-completed workflow. Stresses {@code AtMostOnceRecording} (INV-2): the
     * terminal-state guards must make the redelivered wakeup of a finished instance a no-op (no second terminal step
     * record). Scoped to wakeups only — re-delivering a <em>start</em> would re-surface the documented F-3 re-spawn
     * gap, and re-injecting the engine's own internal step events over-claims a dedup the protocol never promised (both
     * are chaos triage outcomes; see {@code DuplicateCompletedFault}). Chaos-only (not in the reproducible/all sets).
     */
    DUPLICATE_COMPLETED,

    /**
     * Event-store latency jitter: perturbs delivery timing/ordering within the harness's own control — a seeded number
     * of extra virtual-time settle nudges plus a more aggressive shuffle of the pending batch than the single-swap
     * {@link #MESSAGE_REORDER} reorder mode. No engine state is dropped. Stresses {@code DeterministicReplay} (INV-4)
     * and ordering robustness: an instance's per-{@code workflowId} subsequence must stay stable regardless of how the
     * cross-instance global interleaving is jittered (the F-2 surface). Chaos-only.
     */
    EVENT_STORE_LATENCY_JITTER,

    /**
     * Flapping restart: several rapid {@code crashAndRecover} cycles in a row (count drawn from the seeded RNG), with a
     * {@code CommittedHistorySurvivesCrash} (INV-3) re-assertion after each cycle. Stresses
     * {@code CommittedHistorySurvivesCrash} (INV-3) and {@code EventuallyTerminates} (INV-5, resume after repeated
     * restarts) far harder than the single-cycle {@link #RESTART} / {@link #WORKER_CRASH}. Chaos-only.
     */
    FLAPPING_RESTART,

    /**
     * Clock skew: a seed-chosen forward clock jump of a varied magnitude (seconds to many hours), advancing the mutable
     * clock and the virtual-time scheduler in lock-step. Only forward jumps are used — the {@code MutableClock} +
     * {@code ManualWorkflowScheduler} virtual time is monotonic (a backward jump would violate the scheduler's
     * non-decreasing due-time contract), so skew is modelled as forward jumps of varying size rather than direction.
     * Stresses {@code EventuallyTerminates} (INV-5) and the {@code orTimeout} per-attempt timeout math. Chaos-only.
     */
    CLOCK_SKEW,

    /**
     * Partial batch: deliver only a seed-chosen part of this step's pending external-event batch, deferring the rest to
     * a later step. Where {@link #MESSAGE_REORDER}'s delay mode defers one entry, this defers a whole tail of the batch
     * at once. Stresses {@code AtMostOnceRecording} (INV-2) and ordering robustness — a step that sees only part of its
     * inputs this step (and the remainder later) must still record each input's effect at most once. Chaos-only.
     */
    PARTIAL_BATCH,

    /**
     * Duplicated append (Phase 4): the durable store records the next commit TWICE — an at-least-once store, e.g. a
     * retried append whose first attempt actually landed. The duplicate surfaces on the next crash+recovery replay;
     * the live path is unchanged. Stresses the replay-idempotence face of {@code AtMostOnceRecording} (INV-2) and
     * {@code DeterministicReplay} (INV-4) under a duplicated durable event. Chaos-only.
     */
    DUPLICATED_APPEND,

    /**
     * Stale writer: appends one foreign event tagged with a live instance's {@code workflowId} straight to the shared
     * durable store, then lets the next segment claim restore that instance. It is the write a node that lost its
     * segment claim still lands, seen from the store both nodes share, and the only adversary the DCB append condition
     * exists for. The execution holding the instance is rejected on its very next append and stops without a terminal
     * event; the instance parks until the claim that follows restores it past the foreign write. Stresses
     * {@code AtMostOnceRecording} (INV-2, no record from a fenced writer), {@code TerminalIsFinal} (INV-7) and
     * {@code EventuallyTerminates} (INV-5, the parked instance resumes on the next claim).
     */
    STALE_WRITER
}
