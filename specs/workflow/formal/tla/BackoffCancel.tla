--------------------------- MODULE BackoffCancel ---------------------------
(***************************************************************************)
(* Design-level model of the cancellation-during-retry-backoff gap         *)
(* (candidate finding, corroborating the DST BackoffCancelSwallowedTest /  *)
(* BackoffCancelScenario pin).                                             *)
(*                                                                         *)
(* While a retrying step waits out its backoff, the only registered        *)
(* running future is RetryableExecuteDelegate.scheduleRetryAttempt's       *)
(* backoff-launch future. That future has NO cancellation-to-publish       *)
(* wiring: its .exceptionally is chained on the UPSTREAM runAsync stage,   *)
(* so completeExceptionally on the registered (downstream) future runs     *)
(* nothing — and the scheduler's delayed launch task is never              *)
(* unscheduled (the JDK AsyncRun still executes when the delay fires,      *)
(* because the stage it completes is not the stage the engine completed    *)
(* exceptionally). Consequently a cancellation that lands in the backoff   *)
(* window — whether an in-body ctx.cancel/ctx.fail (whose                  *)
(* cancelAllRunningSteps then BLOCKS in awaitStateChange(allTerminal) on a *)
(* step nothing drives terminal) or an external                            *)
(* cancelRunningStep(stepName, cause) — is silently lost: no CANCELLED     *)
(* record is published for the step, and when the backoff elapses the      *)
(* scheduled launch (gated only on "step not terminal", which RETRYING is  *)
(* not) RUNS the action's side effect anyway.                              *)
(*                                                                         *)
(* The model: one retrying step and one cancellation request racing the    *)
(* backoff timer. The fix toggle CANCEL_COVERS_BACKOFF models the          *)
(* candidate fix: a cancellation arriving while the step is RETRYING       *)
(* publishes the step's terminal CANCELLED and unschedules the pending     *)
(* launch (the same terminal-record wiring the in-flight execute action    *)
(* and the parked wait step already have).                                 *)
(*                                                                         *)
(* CancelledStepDoesNotRun: after a cancellation is requested, the step's  *)
(* action side effect must not run again.                                  *)
(*   CANCEL_COVERS_BACKOFF = FALSE -> VIOLATED (MC_backoffcancel.cfg) —    *)
(*                                    the backoff fires after the cancel   *)
(*                                    and the doomed attempt runs its      *)
(*                                    effect (the DST pin's effect 1 -> 2) *)
(*   CANCEL_COVERS_BACKOFF = TRUE  -> holds (MC_backoffcancel_fixed.cfg)   *)
(***************************************************************************)
EXTENDS Naturals

CONSTANT CANCEL_COVERS_BACKOFF  \* TRUE = cancellation of a RETRYING step publishes CANCELLED + unschedules the launch (the modeled fix).

VARIABLES
    step,               \* "started" -> "retrying" -> ("completed" | "cancelled"): the flaky step's durable status
    backoffScheduled,   \* TRUE while the retry launch sits on the scheduler
    cancelRequested,    \* TRUE once a cancellation (in-body terminate or external cancelRunningStep) has been issued
    effectsAfterCancel, \* Nat: action side-effect executions AFTER the cancellation was requested
    workflow            \* "running" -> "cancelPending" -> "cancelled" (the in-body terminate's progress)

vars == <<step, backoffScheduled, cancelRequested, effectsAfterCancel, workflow>>

Init ==
    /\ step = "started"
    /\ backoffScheduled = FALSE
    /\ cancelRequested = FALSE
    /\ effectsAfterCancel = 0
    /\ workflow = "running"

\* Attempt 1 fails: the engine records RETRYING and schedules the retry
\* launch on the (virtual) scheduler — the backoff window opens. This can
\* also happen AFTER a cancellation request (the engine analogue: the
\* attempt's queued failure task is drained by cancelAllRunningSteps'
\* drainPendingTasks and schedules the retry mid-cancel), so the modeled fix
\* must cover this ordering too: a failing attempt under a requested
\* cancellation goes terminal CANCELLED instead of scheduling a retry.
AttemptFails ==
    /\ step = "started"
    /\ IF CANCEL_COVERS_BACKOFF /\ cancelRequested
           THEN step' = "cancelled" /\ backoffScheduled' = FALSE
           ELSE step' = "retrying" /\ backoffScheduled' = TRUE
    /\ UNCHANGED <<cancelRequested, effectsAfterCancel, workflow>>

\* The cancellation request lands (ctx.cancel's cancelAllRunningSteps or an
\* external cancelRunningStep) — during the backoff window or before it.
\* Today (toggle FALSE) completing the backoff future exceptionally publishes
\* NOTHING and unschedules NOTHING; the modeled fix (toggle TRUE) drives the
\* RETRYING step terminal CANCELLED and drops the pending launch.
RequestCancel ==
    /\ workflow = "running"
    /\ cancelRequested' = TRUE
    /\ workflow' = "cancelPending"
    /\ IF CANCEL_COVERS_BACKOFF /\ step = "retrying"
           THEN step' = "cancelled" /\ backoffScheduled' = FALSE
           ELSE UNCHANGED <<step, backoffScheduled>>
    /\ UNCHANGED effectsAfterCancel

\* The backoff elapses: the scheduled launch task runs. Its only gate is
\* "step not terminal" — RETRYING is not terminal, so the attempt executes
\* the action's side effect (and completes the step), cancel or no cancel.
BackoffFires ==
    /\ backoffScheduled
    /\ step = "retrying"
    /\ backoffScheduled' = FALSE
    /\ step' = "completed"
    /\ effectsAfterCancel' = IF cancelRequested THEN effectsAfterCancel + 1
                                                ELSE effectsAfterCancel
    /\ UNCHANGED <<cancelRequested, workflow>>

\* The blocked in-body terminate can only proceed once the step is terminal
\* (cancelAllRunningSteps' awaitStateChange(allTerminal)) — only then does
\* the workflow's own CANCELLED commit.
FinishCancel ==
    /\ workflow = "cancelPending"
    /\ step \in {"completed", "cancelled"}
    /\ workflow' = "cancelled"
    /\ UNCHANGED <<step, backoffScheduled, cancelRequested, effectsAfterCancel>>

Next == AttemptFails \/ RequestCancel \/ BackoffFires \/ FinishCancel

Spec == Init /\ [][Next]_vars

\* THE property: once a cancellation has been requested, the cancelled step's
\* action must not run its side effect again (the DST pin's effect 1 -> 2 is
\* exactly this counter going positive).
CancelledStepDoesNotRun ==
    effectsAfterCancel = 0

=============================================================================
