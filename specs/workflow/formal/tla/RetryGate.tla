------------------------------ MODULE RetryGate ------------------------------
(***************************************************************************)
(* Design-level model of the retry-attempt gate (issue #408 / FND-10,      *)
(* corroborating the DST FencedRetryBackoffTest / FencedRetryBackoffScenario*)
(* pin).                                                                   *)
(*                                                                         *)
(* Under DCB append conditions a step's first attempt runs its action only *)
(* after the store accepted this execution's own STARTED. Before #408 a    *)
(* retry attempt skipped that gate: the step was already present as        *)
(* RETRYING, so when the backoff timer fired the action ran with nothing   *)
(* forcing the store to confirm the attempt was this execution's. A node   *)
(* that lost the instance during the backoff therefore ran the effect on a *)
(* stale marker; only its later outcome append was rejected.               *)
(*                                                                         *)
(* The model: writer A recorded RETRYING and waits out the backoff. The    *)
(* claim may move to writer B, which restores the instance from the log    *)
(* and drives the retry itself. Every accepted append moves the store head *)
(* past A's marker (headMoved), so any later append by A is rejected.      *)
(*                                                                         *)
(* The fix toggle RETRY_NEEDS_ACCEPTED_APPEND models #408: a retry attempt *)
(* publishes its own RETRY_STARTED and runs its action only when the store *)
(* accepted that append.                                                   *)
(*                                                                         *)
(* RetryAttemptGated: a retry attempt runs its action only after an        *)
(* accepted append of its own.                                             *)
(*   RETRY_NEEDS_ACCEPTED_APPEND = FALSE -> VIOLATED (MC_retrygate.cfg) -- *)
(*       A's backoff fires after B wrote and A runs the effect anyway      *)
(*       (the DST pin's effects 1 -> 2)                                    *)
(*   RETRY_NEEDS_ACCEPTED_APPEND = TRUE  -> holds (MC_retrygate_fixed.cfg) *)
(***************************************************************************)
EXTENDS Naturals

CONSTANT RETRY_NEEDS_ACCEPTED_APPEND  \* TRUE = a retry attempt appends RETRY_STARTED first and runs only if accepted (#408).

VARIABLES
    step,             \* "retrying" -> ("retryStarted" ->) "completed": the step's durable status
    owner,            \* "A" or "B": which writer holds the claim on the instance
    backoffPending,   \* TRUE while A's retry launch sits on the scheduler
    headMoved,        \* TRUE once an accepted append moved the store head past A's marker
    effectsUngated    \* Nat: retry actions run by a writer whose marker the store no longer accepts

vars == <<step, owner, backoffPending, headMoved, effectsUngated>>

\* Attempt 1 by A failed: RETRYING is recorded, the backoff window is open.
Init ==
    /\ step = "retrying"
    /\ owner = "A"
    /\ backoffPending = TRUE
    /\ headMoved = FALSE
    /\ effectsUngated = 0

\* The claim moves during the backoff. B restores the instance from the log
\* (RETRYING, attempt pending) and will drive the retry itself.
ClaimMoves ==
    /\ owner = "A"
    /\ backoffPending
    /\ owner' = "B"
    /\ UNCHANGED <<step, backoffPending, headMoved, effectsUngated>>

\* With the fix, B's retry attempt appends RETRY_STARTED before its action.
\* The accepted append moves the head past A's marker.
BRetryStarts ==
    /\ RETRY_NEEDS_ACCEPTED_APPEND
    /\ owner = "B"
    /\ step = "retrying"
    /\ step' = "retryStarted"
    /\ headMoved' = TRUE
    /\ UNCHANGED <<owner, backoffPending, effectsUngated>>

\* B's retry attempt completes. Without the fix B runs the action straight
\* from RETRYING; with the fix it runs after its RETRY_STARTED. Either way
\* B's marker is current, so its COMPLETED is accepted and the head moves.
BCompletes ==
    /\ owner = "B"
    /\ IF RETRY_NEEDS_ACCEPTED_APPEND THEN step = "retryStarted" ELSE step = "retrying"
    /\ step' = "completed"
    /\ headMoved' = TRUE
    /\ UNCHANGED <<owner, backoffPending, effectsUngated>>

\* A's backoff elapses and its retry launch runs.
\*   Without the fix: the action runs at once (step is RETRYING, not terminal);
\*   if the head moved, that effect ran on a stale marker and A's outcome
\*   append is rejected, so the log does not change.
\*   With the fix: A first appends RETRY_STARTED. If the head moved the append
\*   is rejected and nothing runs; otherwise the attempt runs and completes.
BackoffFires ==
    /\ backoffPending
    /\ backoffPending' = FALSE
    /\ IF RETRY_NEEDS_ACCEPTED_APPEND
           THEN IF headMoved
                    THEN UNCHANGED <<step, headMoved, effectsUngated>>
                    ELSE step' = "completed" /\ headMoved' = TRUE /\ UNCHANGED effectsUngated
           ELSE IF headMoved
                    THEN effectsUngated' = effectsUngated + 1 /\ UNCHANGED <<step, headMoved>>
                    ELSE step' = "completed" /\ headMoved' = TRUE /\ UNCHANGED effectsUngated
    /\ UNCHANGED owner

Next == ClaimMoves \/ BRetryStarts \/ BCompletes \/ BackoffFires

Spec == Init /\ [][Next]_vars

\* THE property: a retry attempt runs its action only after an accepted
\* append of its own (the DST pin's effects 1 -> 2 is this counter going
\* positive).
RetryAttemptGated ==
    effectsUngated = 0

=============================================================================
