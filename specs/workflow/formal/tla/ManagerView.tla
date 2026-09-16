----------------------------- MODULE ManagerView -----------------------------
(***************************************************************************)
(* Design-level model of the Workflow Manager's two-source read (PR #452,  *)
(* ADR-018), corroborating the DST ManagerVisibilityProbeTest /            *)
(* ManagerVisibilityProbeScenario pin.                                     *)
(*                                                                         *)
(* SimpleWorkflowManager answers a query from two repositories and merges  *)
(* them by workflow id: the live WorkflowExecutionRepository, holding the  *)
(* executions this engine currently runs, and the WorkflowHistoryRepository*)
(* a projection the WorkflowHistoryProjector builds from the event stream  *)
(* on the pooled streaming processor. Live wins when both hold the id.     *)
(* WorkflowEngine.removeExecution drops a finished execution from the live *)
(* repository the moment its body finishes, and a crash empties the live   *)
(* repository until the restore re-adds the still-running instances. The   *)
(* projector lags the log by an unbounded number of events. Nothing ties   *)
(* the removal or the crash to the projector's position.                   *)
(*                                                                         *)
(* The model: one engine, a bounded set of workflow ids, a durable log per *)
(* id reduced to its status and step count, the live set, the projector's  *)
(* position per id, and the last state the manager returned per id.       *)
(*                                                                         *)
(* The fix toggle RETAIN_LIVE_UNTIL_PROJECTED models an engine-side fix:   *)
(* a finished execution leaves the live repository only once the projector *)
(* has applied its terminal event. The fix toggle READ_LOG_ON_HISTORY_MISS *)
(* models a manager-side fix: when the id is not live, the manager sources *)
(* the state from the event store instead of trusting the projection.      *)
(*                                                                         *)
(* ManagerVisibilityMonotonic: once the Workflow Manager has returned a    *)
(* state for a workflow id, every later query for that id returns a state. *)
(*   both toggles FALSE            -> VIOLATED (MC_managerview.cfg) --     *)
(*       the finished execution is removed before the projector applied    *)
(*       its STARTED, so the id vanishes from the manager                  *)
(*   RETAIN_LIVE_UNTIL_PROJECTED   -> VIOLATED (MC_managerview_retain.cfg) *)
(*       -- retention closes the removal window but a crash still empties  *)
(*       the live repository ahead of the projector                        *)
(*   READ_LOG_ON_HISTORY_MISS      -> holds (MC_managerview_fixed.cfg)     *)
(*                                                                         *)
(* ManagerStatusMonotonic: once the Workflow Manager has returned a        *)
(* terminal state for a workflow id, every later query for that id returns *)
(* that terminal state.                                                    *)
(*   both toggles FALSE            -> VIOLATED (MC_managerview_status.cfg) *)
(*       -- the live COMPLETED state is removed and the lagging projection *)
(*       answers STARTED                                                   *)
(*   READ_LOG_ON_HISTORY_MISS      -> holds (MC_managerview_fixed.cfg)     *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Workflows,                    \* the bounded set of workflow ids
    MAX_STEPS,                    \* Nat: step records a workflow may append before it completes
    MAX_CRASHES,                  \* Nat: engine crashes the run may suffer
    RETAIN_LIVE_UNTIL_PROJECTED,  \* TRUE = a finished execution leaves the live repository only once projected (engine-side fix)
    READ_LOG_ON_HISTORY_MISS      \* TRUE = a non-live id is answered from the event store, never from the projection (manager-side fix)

VARIABLES
    status,     \* [Workflows -> {"absent", "started", "completed"}]: the durable log status per id
    steps,      \* [Workflows -> 0..MAX_STEPS]: step records appended per id
    live,       \* SUBSET Workflows: ids held by the live WorkflowExecutionRepository
    projected,  \* [Workflows -> Nat]: log events of that id the WorkflowHistoryProjector has applied
    up,         \* BOOLEAN: FALSE between a crash and the restore
    crashes,    \* Nat: crashes so far
    lastView    \* [Workflows -> {"none", "absent", "started", "completed"}]: the manager's last answer per id

vars == <<status, steps, live, projected, up, crashes, lastView>>

Status == {"absent", "started", "completed"}

\* Durable events of an id: STARTED, its steps, and COMPLETED when terminal.
LogLen(w) ==
    (IF status[w] = "absent" THEN 0 ELSE 1)
    + steps[w]
    + (IF status[w] = "completed" THEN 1 ELSE 0)

\* What the history projection answers for an id: nothing before its STARTED
\* is applied, COMPLETED only once every event including the terminal one is
\* applied, STARTED otherwise.
HistoryStatus(w) ==
    IF projected[w] = 0 THEN "absent"
    ELSE IF status[w] = "completed" /\ projected[w] = LogLen(w) THEN "completed"
    ELSE "started"

\* What SimpleWorkflowManager answers now: live wins, then the projection,
\* or the event store when the manager-side fix is on.
View(w) ==
    IF w \in live THEN status[w]
    ELSE IF READ_LOG_ON_HISTORY_MISS THEN status[w]
    ELSE HistoryStatus(w)

TypeOK ==
    /\ status \in [Workflows -> Status]
    /\ steps \in [Workflows -> 0..MAX_STEPS]
    /\ live \subseteq Workflows
    /\ projected \in [Workflows -> 0..(MAX_STEPS + 2)]
    /\ up \in BOOLEAN
    /\ crashes \in 0..MAX_CRASHES
    /\ lastView \in [Workflows -> Status \cup {"none"}]
    /\ \A w \in Workflows : projected[w] <= LogLen(w)

Init ==
    /\ status = [w \in Workflows |-> "absent"]
    /\ steps = [w \in Workflows |-> 0]
    /\ live = {}
    /\ projected = [w \in Workflows |-> 0]
    /\ up = TRUE
    /\ crashes = 0
    /\ lastView = [w \in Workflows |-> "none"]

\* The engine handles a start event: STARTED is durable and the execution is
\* saved in the live repository.
Start(w) ==
    /\ up
    /\ status[w] = "absent"
    /\ status' = [status EXCEPT ![w] = "started"]
    /\ live' = live \cup {w}
    /\ UNCHANGED <<steps, projected, up, crashes, lastView>>

\* A running body appends one step record.
Step(w) ==
    /\ up
    /\ w \in live
    /\ status[w] = "started"
    /\ steps[w] < MAX_STEPS
    /\ steps' = [steps EXCEPT ![w] = steps[w] + 1]
    /\ UNCHANGED <<status, live, projected, up, crashes, lastView>>

\* The body finishes: COMPLETED is durable and applied to the live state. The
\* execution is still in the live repository until the finished callback runs.
Complete(w) ==
    /\ up
    /\ w \in live
    /\ status[w] = "started"
    /\ status' = [status EXCEPT ![w] = "completed"]
    /\ UNCHANGED <<steps, live, projected, up, crashes, lastView>>

\* WorkflowEngine.removeExecution: the finished callback drops the execution
\* from the live repository. With the engine-side fix it waits for the
\* projector to have applied the terminal event.
Remove(w) ==
    /\ up
    /\ w \in live
    /\ status[w] = "completed"
    /\ RETAIN_LIVE_UNTIL_PROJECTED => projected[w] = LogLen(w)
    /\ live' = live \ {w}
    /\ UNCHANGED <<status, steps, projected, up, crashes, lastView>>

\* WorkflowHistoryProjector applies the next durable event of an id.
Project(w) ==
    /\ projected[w] < LogLen(w)
    /\ projected' = [projected EXCEPT ![w] = projected[w] + 1]
    /\ UNCHANGED <<status, steps, live, up, crashes, lastView>>

\* The engine crashes: the in-memory live repository is gone. The log and the
\* projection survive.
Crash ==
    /\ up
    /\ crashes < MAX_CRASHES
    /\ up' = FALSE
    /\ live' = {}
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<status, steps, projected, lastView>>

\* The restore re-adds every still-running instance from the event store.
Restore ==
    /\ ~up
    /\ up' = TRUE
    /\ live' = {w \in Workflows : status[w] = "started"}
    /\ UNCHANGED <<status, steps, projected, crashes, lastView>>

\* An outside-in caller asks the manager for one id and keeps the answer.
Query(w) ==
    /\ lastView' = [lastView EXCEPT ![w] = View(w)]
    /\ UNCHANGED <<status, steps, live, projected, up, crashes>>

Next ==
    \/ \E w \in Workflows : Start(w) \/ Step(w) \/ Complete(w) \/ Remove(w) \/ Project(w) \/ Query(w)
    \/ Crash
    \/ Restore

Spec == Init /\ [][Next]_vars

\* THE property: once the Workflow Manager has returned a state for a
\* workflow id, every later query for that id returns a state.
ManagerVisibilityMonotonic ==
    \A w \in Workflows :
        lastView[w] \in {"started", "completed"} => View(w) /= "absent"

\* THE property: once the Workflow Manager has returned a terminal state for
\* a workflow id, every later query for that id returns that terminal state.
ManagerStatusMonotonic ==
    \A w \in Workflows :
        lastView[w] = "completed" => View(w) = "completed"

=============================================================================
