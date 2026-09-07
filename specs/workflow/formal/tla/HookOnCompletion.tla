--------------------------- MODULE HookOnCompletion ---------------------------
(***************************************************************************)
(* Design-level model of the terminal lifecycle-hook drop on the happy     *)
(* completion path (finding F-4, corroborating the DST                     *)
(* Inv17StatusHookFiresOncePerStatusTest /                                 *)
(* StatusHookFiresOncePerStatusScenario pin —                              *)
(* documentTerminalHookMayBeDropped).                                      *)
(*                                                                         *)
(* The engine's happy completion path (SimpleWorkflowExecution             *)
(* .executeWorkflow) sends the COMPLETED workflow-status event and awaits  *)
(* only its COMMIT — not the state change. The event's live application    *)
(* (which runs setStatus(COMPLETED) and NOTIFIES the registered            *)
(* status-change listener) is a QUEUED task on the per-instance task       *)
(* queue. The body then returns and finishWorkflow runs taskQueue.clear(), *)
(* which can clear the queued notify task BEFORE it runs — the COMPLETED   *)
(* lifecycle hook fires ZERO times (a LOST fire, the opposite direction    *)
(* from an F-0-style re-fire; the no-re-fire facet of INV-17 holds). The   *)
(* fail/cancel/timeout terminal paths DO awaitStateChange after sending    *)
(* their status event, so their hooks fire reliably.                       *)
(*                                                                         *)
(* The model: one instance terminates via either the HAPPY path or a       *)
(* TERMINAL-EXCEPTION path (one representative of fail/cancel/timeout).    *)
(* SendTerminalStatus commits the terminal status event and enqueues the   *)
(* notify task; the race is RunNotifyTask (the hook fires) vs              *)
(* FinishWorkflow's queue clear. The exception path always awaits the      *)
(* state change before the clear; the happy path awaits it only under the  *)
(* fix toggle AWAIT_TERMINAL_STATE (the candidate fix: awaitStateChange on *)
(* the terminal status before finishWorkflow, symmetric with the awaited   *)
(* terminal paths).                                                        *)
(*                                                                         *)
(* StatusHookFiresOncePerStatus: the terminal-status hook never fires more *)
(* than once, and by the time the instance is finished it has fired        *)
(* exactly once.                                                           *)
(*   AWAIT_TERMINAL_STATE = FALSE -> VIOLATED (MC_hook.cfg) — the happy    *)
(*                                   path clears the queued notify task    *)
(*                                   before it runs (hook fired 0 times)   *)
(*   AWAIT_TERMINAL_STATE = TRUE  -> holds    (MC_hook_fixed.cfg)          *)
(***************************************************************************)
EXTENDS Naturals

CONSTANT AWAIT_TERMINAL_STATE  \* TRUE = the happy path also awaits the terminal state change before finishWorkflow.

VARIABLES
    path,      \* "happy" (executeWorkflow completion) or "exception" (fail/cancel/timeout)
    phase,     \* "running" -> "sent" (terminal status committed, notify queued) -> "finished"
    queued,    \* the live-apply notify task is on the per-instance task queue
    hookFires  \* how many times the terminal-status hook has fired

vars == <<path, phase, queued, hookFires>>

Init ==
    /\ path \in {"happy", "exception"}
    /\ phase = "running"
    /\ queued = FALSE
    /\ hookFires = 0

\* The terminal path sends its workflow-status event and awaits its COMMIT.
\* The event's live application — setStatus(terminal) -> notify(listener) —
\* is enqueued as a task on the per-instance queue.
SendTerminalStatus ==
    /\ phase = "running"
    /\ phase' = "sent"
    /\ queued' = TRUE
    /\ UNCHANGED <<path, hookFires>>

\* The queued live-apply task is consumed: setStatus(terminal) NOTIFIES the
\* registered status-change listener — the hook fires.
RunNotifyTask ==
    /\ queued
    /\ queued' = FALSE
    /\ hookFires' = hookFires + 1
    /\ UNCHANGED <<path, phase>>

\* Does this path awaitStateChange on the terminal status before the body
\* returns? The exception (fail/cancel/timeout) path always does; the happy
\* path only under the modeled fix.
MustAwaitStateChange == path = "exception" \/ AWAIT_TERMINAL_STATE

\* The body returns and finishWorkflow runs taskQueue.clear(). On an awaited
\* path this can only happen once the notify task has run; on the un-awaited
\* happy path the clear can race ahead and drop the queued notify task.
FinishWorkflow ==
    /\ phase = "sent"
    /\ MustAwaitStateChange => ~queued
    /\ queued' = FALSE   \* taskQueue.clear() — a still-queued notify task is dropped
    /\ phase' = "finished"
    /\ UNCHANGED <<path, hookFires>>

Next == SendTerminalStatus \/ RunNotifyTask \/ FinishWorkflow

Spec == Init /\ [][Next]_vars

\* THE property (INV-17 / F-4): the terminal-status hook fires at most once at
\* every point, and by the time the instance is finished it has fired exactly
\* once for the terminal status.
StatusHookFiresOncePerStatus ==
    /\ hookFires <= 1
    /\ (phase = "finished") => (hookFires = 1)

=============================================================================
