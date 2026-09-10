---------------------------- MODULE PublishRouting ----------------------------
(***************************************************************************)
(* Design-level model of the publish primitive (ADR-019):                  *)
(* WorkflowContext#publish appends ONE event that is both a business event *)
(* and the publisher's COMPLETED step. Three engine decisions carry it:    *)
(*                                                                         *)
(*  1. Routing. The event carries workflowId metadata, which ADR-014       *)
(*     routes to the owning segment only. A published event is instead     *)
(*     always BROADCAST (WorkflowEngineSequencingPolicy), because it must  *)
(*     reach (a) the publisher's own segment so the publisher observes its *)
(*     step, (b) the segment of any instance the event starts, and (c) the *)
(*     segment of any instance whose waitForEvent it matches. Candidate    *)
(*     routing would deliver it to (b) alone.                              *)
(*  2. Foreign-instance guard. Broadcast evolves every owned instance from *)
(*     the event; EventSourcedWorkflowState.evolve ignores step metadata   *)
(*     carrying another instance's workflowId.                             *)
(*  3. Replay-skip gate. PublishDelegate gates the append on               *)
(*     !containsStep(stepName), so a post-crash re-run never appends the   *)
(*     event a second time (the F-7 lesson, PayloadRepublish.tla).         *)
(*                                                                         *)
(* Four instances on three segments: the publisher P (s0), a candidate C   *)
(* the published event starts (s1), and two running waiters whose          *)
(* waitForEvent matches the published event -- waiterA on a third segment  *)
(* (s2) and waiterB on the publisher's own segment (s0), so a 1:N wake     *)
(* across and within segments is covered. A waiter may register its wait   *)
(* before or after the event passes its segment: a wait registered after   *)
(* the event went by is not woken -- wait conditions are evaluated at live *)
(* delivery only. That is the engine's documented semantics, pinned here   *)
(* as LateWaitNeverWakes, not a defect.                                    *)
(*                                                                         *)
(* Each CONSTANT is TRUE for the code as written and FALSE as a deliberate *)
(* MUTATION, so every invariant is shown to be non-vacuous:                *)
(*   BROADCAST_PUBLISH  = FALSE -> PublisherObservesOwnPublish and         *)
(*                                 WakeExactlyOnce VIOLATED                *)
(*   FOREIGN_ID_GUARD   = FALSE -> NoForeignStepRecorded VIOLATED          *)
(*   CONTAINS_STEP_GATE = FALSE -> AtMostOnceRecording VIOLATED            *)
(* Standalone; run with -deadlock (the bounded run terminates).            *)
(***************************************************************************)
EXTENDS Naturals, TLC

CONSTANTS
    BROADCAST_PUBLISH,   \* TRUE = published events are BROADCAST; FALSE = candidate-routed to C's segment only
    FOREIGN_ID_GUARD,    \* TRUE = evolve ignores step metadata of another workflowId; FALSE = registers it
    CONTAINS_STEP_GATE   \* TRUE = re-run skips the publish when the step is in state; FALSE = re-publishes

Segments == {"s0", "s1", "s2"}
Waiters  == {"waiterA", "waiterB"}
Others   == {"candidate"} \cup Waiters

PublisherSegment == "s0"
CandidateSegment == "s1"
WaiterSegment    == ("waiterA" :> "s2") @@ ("waiterB" :> "s0")
WaitersOn(s)     == {w \in Waiters : WaiterSegment[w] = s}

\* Where the processor delivers the published event (WorkflowEngineSequencingPolicy.sequenceIdentifierFor).
Routed(s) == IF BROADCAST_PUBLISH THEN TRUE ELSE s = CandidateSegment

VARIABLES
    phase,       \* publisher body: "live" -> "committed" -> ("finished" | "crashed" -> "rerun" -> "finished")
    records,     \* publish-step COMPLETED records in the durable log (the published event itself)
    passed,      \* [Segments -> BOOLEAN]: the segment's processor moved past the published event
    stepSeen,    \* the publisher's own state registered the step (its awaitStateChange unblocks)
    spawned,     \* executions created for the candidate
    foreignStep, \* [Others -> BOOLEAN]: the instance registered the publisher's step as its own
    waiting,     \* [Waiters -> BOOLEAN]: the waiter has registered its waitForEvent
    waitedLate,  \* [Waiters -> BOOLEAN]: the waiter registered after the event had passed its segment
    dueWakes,    \* [Waiters -> Nat]: wakes the waiter is owed: it was waiting when the event reached its segment
    doneWakes    \* [Waiters -> Nat]: wakes actually applied to the waiter

vars == << phase, records, passed, stepSeen, spawned, foreignStep, waiting, waitedLate, dueWakes, doneWakes >>

TypeOK ==
    /\ phase \in {"live", "committed", "crashed", "rerun", "finished"}
    /\ records \in Nat
    /\ passed \in [Segments -> BOOLEAN]
    /\ stepSeen \in BOOLEAN
    /\ spawned \in Nat
    /\ foreignStep \in [Others -> BOOLEAN]
    /\ waiting \in [Waiters -> BOOLEAN]
    /\ waitedLate \in [Waiters -> BOOLEAN]
    /\ dueWakes \in [Waiters -> Nat]
    /\ doneWakes \in [Waiters -> Nat]

Init ==
    /\ phase = "live"
    /\ records = 0
    /\ passed = [s \in Segments |-> FALSE]
    /\ stepSeen = FALSE
    /\ spawned = 0
    /\ foreignStep = [i \in Others |-> FALSE]
    /\ waiting = [w \in Waiters |-> FALSE]
    /\ waitedLate = [w \in Waiters |-> FALSE]
    /\ dueWakes = [w \in Waiters |-> 0]
    /\ doneWakes = [w \in Waiters |-> 0]

\* WaitForDelegate.waitForEvent: a waiter registers its in-memory wait condition, at any time.
RegisterWait(w) ==
    /\ ~waiting[w]
    /\ waiting' = [waiting EXCEPT ![w] = TRUE]
    /\ waitedLate' = [waitedLate EXCEPT ![w] = passed[WaiterSegment[w]]]
    /\ UNCHANGED << phase, records, passed, stepSeen, spawned, foreignStep, dueWakes, doneWakes >>

\* PublishDelegate.publish, first live run: the event durably commits.
Publish ==
    /\ phase = "live"
    /\ records' = 1
    /\ phase' = "committed"
    /\ UNCHANGED << passed, stepSeen, spawned, foreignStep, waiting, waitedLate, dueWakes, doneWakes >>

\* The processor of segment s reaches the published event. Routed(s) decides whether WorkflowEngine.handle
\* runs for it there (Deliver) or the event goes by unseen (Skip).
Deliver(s) ==
    /\ records >= 1
    /\ ~passed[s]
    /\ Routed(s)
    /\ passed' = [passed EXCEPT ![s] = TRUE]
    \* the publisher is one of the owned executions on its segment: its state evolves the step
    /\ stepSeen' = IF s = PublisherSegment THEN TRUE ELSE stepSeen
    \* checkAndCreateNewInstance starts the candidate on the segment that owns it; the new instance is then
    \* evolved from the same event like every owned execution
    /\ spawned' = IF s = CandidateSegment THEN spawned + 1 ELSE spawned
    /\ foreignStep' = [i \in Others |->
                          IF (i = "candidate" /\ s = CandidateSegment)
                             \/ (i \in Waiters /\ WaiterSegment[i] = s /\ waiting[i])
                          THEN ~FOREIGN_ID_GUARD ELSE foreignStep[i]]
    \* EventWaitConditions.evaluateAndApply: every registered wait on this segment is matched now
    /\ dueWakes'  = [w \in Waiters |-> IF WaiterSegment[w] = s /\ waiting[w] THEN dueWakes[w] + 1 ELSE dueWakes[w]]
    /\ doneWakes' = [w \in Waiters |-> IF WaiterSegment[w] = s /\ waiting[w] THEN doneWakes[w] + 1 ELSE doneWakes[w]]
    /\ UNCHANGED << phase, records, waiting, waitedLate >>

Skip(s) ==
    /\ records >= 1
    /\ ~passed[s]
    /\ ~Routed(s)
    /\ passed' = [passed EXCEPT ![s] = TRUE]
    \* every waiter on this segment was owed this wake: the event passed while it was waiting
    /\ dueWakes' = [w \in Waiters |-> IF WaiterSegment[w] = s /\ waiting[w] THEN dueWakes[w] + 1 ELSE dueWakes[w]]
    /\ UNCHANGED << phase, records, stepSeen, spawned, foreignStep, waiting, waitedLate, doneWakes >>

\* The publisher's body continues only once its own state holds the step.
Finish ==
    /\ phase \in {"committed", "rerun"}
    /\ stepSeen
    /\ phase' = "finished"
    /\ UNCHANGED << records, passed, stepSeen, spawned, foreignStep, waiting, waitedLate, dueWakes, doneWakes >>

\* Crash after the event committed and before the publisher's terminal commit: recovery re-runs the body.
Crash ==
    /\ phase = "committed"
    /\ phase' = "crashed"
    /\ UNCHANGED << records, passed, stepSeen, spawned, foreignStep, waiting, waitedLate, dueWakes, doneWakes >>

\* The recovered body re-reaches the publish call with the step sourced from the durable log.
Rerun ==
    /\ phase = "crashed"
    /\ records' = IF CONTAINS_STEP_GATE THEN records ELSE records + 1
    /\ stepSeen' = TRUE   \* sourcing the durable log rebuilds the step into the publisher's state
    /\ phase' = "rerun"
    /\ UNCHANGED << passed, spawned, foreignStep, waiting, waitedLate, dueWakes, doneWakes >>

Next ==
    \/ \E w \in Waiters : RegisterWait(w)
    \/ Publish
    \/ \E s \in Segments : Deliver(s) \/ Skip(s)
    \/ Finish
    \/ Crash
    \/ Rerun

Spec == Init /\ [][Next]_vars

\* INV-2: at most one terminal record per step in the durable log.
AtMostOnceRecording == records <= 1

\* INV-29: an instance evolved from a published event never registers the publisher's step as its own.
NoForeignStepRecorded == \A i \in Others : ~foreignStep[i]

\* INV-29: once the publisher's segment has processed the published event, the publisher's state holds the step.
PublisherObservesOwnPublish == (passed[PublisherSegment] /\ records >= 1) => stepSeen

\* INV-28 facet: every waiter owed a wake by the published event is woken exactly once (1:N).
WakeExactlyOnce == \A w \in Waiters : doneWakes[w] = dueWakes[w]

\* Documented semantics, not a defect: a wait registered after the event passed its segment is never woken.
LateWaitNeverWakes == \A w \in Waiters : waitedLate[w] => doneWakes[w] = 0

\* INV-10 / INV-28 facet: the published event starts the candidate exactly once.
SpawnAtMostOnce == spawned <= 1

=============================================================================
