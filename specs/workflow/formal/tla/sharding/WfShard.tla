------------------------------- MODULE WfShard -------------------------------
(***************************************************************************)
(* Workflow-instance ownership, spawning and wake protocol of the Axoniq    *)
(* workflow engine under segment sharding.                                  *)
(*                                                                          *)
(* Modelled from:                                                           *)
(*   execution/SegmentedWorkflowRouting.java  (ownership + BROADCAST)       *)
(*   execution/WorkflowEngine.java            (handle / claimSegment /      *)
(*                                             releaseSegment /             *)
(*                                             removeTerminalAndStart...)   *)
(*   execution/WorkflowEngineReplaySupport.java (single engine-wide         *)
(*                                               liveMode field)            *)
(*   configuration/WorkflowEventProcessingRegistrationEnhancer.java         *)
(*                                             (SegmentChangeListener)      *)
(*                                                                          *)
(* Segment claim mutual exclusion is ASSUMED, not modelled (holder is a     *)
(* function, so at most one node holds a segment).  That is deliberately    *)
(* the other model's subject.                                               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    ScopedClaimRestore,         \* TRUE  = claim-time restore starts only the claimed segment's instances
                                \* FALSE = code as written: removeTerminalAndStart... scans the whole repository
    ClaimRestoreChecksLiveMode, \* TRUE  = claim-time restore refuses to start while the segment still replays
                                \* FALSE = code as written: claimSegment never consults liveMode
    PerSegmentLiveMode,         \* TRUE  = live mode tracked per segment
                                \* FALSE = code as written: one engine-wide liveMode flag
    SyncRelease,                \* TRUE  = releaseSegment waits for interrupts to drain
                                \* FALSE = code as written: interrupt() is asynchronous
    ExtraSegment,               \* TRUE  = 3 segments / 3 instances / 7 events
    OwnershipGuards             \* TRUE  = code as written: shouldHandle / shouldSpawn filter the
                                \* broadcast fan-in.  FALSE is a deliberate MUTATION used only to
                                \* prove SpawnExactlyOnce / WakeExactlyOnce are not vacuous.

NoNode == "off"

Nodes     == {"n1", "n2"}
Segments  == IF ExtraSegment THEN {"s0", "s1", "s2"} ELSE {"s0", "s1"}
Instances == IF ExtraSegment THEN {"w1", "w2", "w3"} ELSE {"w1", "w2"}
Owner     == ("w1" :> "s0") @@ ("w2" :> "s1") @@ ("w3" :> "s2")

(* Event kinds, mirroring sequenceIdentifierFor:
     "start"  business event with exactly ONE spawn candidate -> routed to Owner[w]
     "bstart" start event whose candidates could not be derived -> BROADCAST
     "wake"   business event with no spawn candidate           -> BROADCAST
     "engine" event carrying workflowId metadata               -> routed to Owner[w] *)
Base == << [k |-> "bstart", w |-> "w1"],
           [k |-> "start",  w |-> "w2"],
           [k |-> "wake",   w |-> "w1"],
           [k |-> "wake",   w |-> "w2"],
           [k |-> "engine", w |-> "w1"] >>

Stream == IF ExtraSegment
          THEN Base \o << [k |-> "bstart", w |-> "w3"], [k |-> "wake", w |-> "w3"] >>
          ELSE Base

ReplayBoundary == 2      \* startupLatestToken: indices 1..1 are replay, >= 2 is live territory
MaxReleases    == 1      \* exactly one handover, keeps the state space finite and small

VARIABLES
    pos,        \* pos[s]: segment token position in the stream
    holder,     \* holder[s]: node holding segment s, or NoNode
    store,      \* store[w]: durable, event-sourced state: "none" / "active" / "terminal"
    res,        \* res[n][w]: repository entry on node n: "none" / "idle" / "running"
    orphan,     \* orphan[n][w]: an interrupted body still draining after releaseSegment
    live,       \* live[n][s]: replay catch-up bit (see IsLive for how it is read)
    spawned,    \* spawned[w]: number of executions actually created for w
    spawnSeen,  \* spawnSeen[w]: 1 once the owning segment consumed a start event for w
    dueWakes,   \* dueWakes[w]: live-region deliveries that must reach w exactly once
    doneWakes,  \* doneWakes[w]: those actually applied to a running execution
    commits,    \* commits[w]: number of terminal commits (double completion detector)
    actedBy,    \* actedBy[w]: set of segments that performed an action on w
    releases

vars == << pos, holder, store, res, orphan, live, spawned, spawnSeen,
           dueWakes, doneWakes, commits, actedBy, releases >>

OwnedBy(s) == {w \in Instances : Owner[w] = s}

\* One engine-wide flag (code as written) vs a per-segment flag (the fix).
IsLive(n, s) == IF PerSegmentLiveMode THEN live[n][s] ELSE \E t \in Segments : live[n][t]

\* removeTerminalAndStartRestoredWorkflowExecutions: starts every non-running execution in scope.
StartIdle(f, S) == [w \in Instances |-> IF w \in S /\ f[w] = "idle" THEN "running" ELSE f[w]]

Init ==
    /\ pos       = [s \in Segments  |-> 0]
    /\ holder    = [s \in Segments  |-> NoNode]
    /\ store     = [w \in Instances |-> "none"]
    /\ res       = [n \in Nodes |-> [w \in Instances |-> "none"]]
    /\ orphan    = [n \in Nodes |-> [w \in Instances |-> FALSE]]
    /\ live      = [n \in Nodes |-> [s \in Segments |-> FALSE]]
    /\ spawned   = [w \in Instances |-> 0]
    /\ spawnSeen = [w \in Instances |-> 0]
    /\ dueWakes  = [w \in Instances |-> 0]
    /\ doneWakes = [w \in Instances |-> 0]
    /\ commits   = [w \in Instances |-> 0]
    /\ actedBy   = [w \in Instances |-> {}]
    /\ releases  = 0

(***************************************************************************)
(* WorkflowEngine.handle: one event delivered to one segment.               *)
(***************************************************************************)
Deliver(n, s) ==
    /\ holder[s] = n
    /\ pos[s] < Len(Stream)
    /\ LET i        == pos[s] + 1
           e        == Stream[i]
           wid      == e.w
           owns     == IF OwnershipGuards THEN Owner[wid] = s   \* shouldHandle / shouldSpawn
                       ELSE Owner[wid] = s \/ e.k \in {"bstart", "wake"}
           wasLive  == IsLive(n, s)                      \* read BEFORE advanceReplayPosition
           \* checkAndCreateNewWorkflow; the duplicate check is repository-only
           spawns   == /\ e.k \in {"start", "bstart"}
                       /\ owns
                       /\ res[n][wid] = "none"
           r1       == IF spawns
                       THEN [res[n] EXCEPT ![wid] = IF wasLive THEN "running" ELSE "idle"]
                       ELSE res[n]
           \* wake loop / workflowId path: only a running execution takes the live path
           due      == /\ e.k \in {"wake", "engine"}
                       /\ Owner[wid] = s
                       /\ i > ReplayBoundary
                       /\ store[wid] = "active"
           done     == /\ e.k \in {"wake", "engine"}
                       /\ owns
                       /\ i > ReplayBoundary
                       /\ store[wid] = "active"
                       /\ r1[wid] = "running"
           \* advanceReplayPosition -> switchToLiveMode -> onLiveModeActivated (fires once)
           justLive == (i >= ReplayBoundary) /\ ~wasLive
           startSet == IF ~justLive THEN {}
                       ELSE IF PerSegmentLiveMode THEN OwnedBy(s) ELSE Instances
           r2       == StartIdle(r1, startSet)
       IN /\ pos'       = [pos EXCEPT ![s] = i]
          /\ res'       = [res EXCEPT ![n] = r2]
          /\ store'     = IF spawns /\ store[wid] # "active"
                          THEN [store EXCEPT ![wid] = "active"] ELSE store
          /\ spawned'   = IF spawns THEN [spawned EXCEPT ![wid] = @ + 1] ELSE spawned
          /\ spawnSeen' = IF e.k \in {"start", "bstart"} /\ owns
                          THEN [spawnSeen EXCEPT ![wid] = 1] ELSE spawnSeen
          /\ dueWakes'  = IF due  THEN [dueWakes  EXCEPT ![wid] = @ + 1] ELSE dueWakes
          /\ doneWakes' = IF done THEN [doneWakes EXCEPT ![wid] = @ + 1] ELSE doneWakes
          /\ actedBy'   = [w \in Instances |->
                             IF (w = wid /\ (spawns \/ done))
                                \/ (r2[w] = "running" /\ r1[w] = "idle")
                             THEN actedBy[w] \cup {s} ELSE actedBy[w]]
          /\ live'      = IF i >= ReplayBoundary THEN [live EXCEPT ![n][s] = TRUE] ELSE live
    /\ UNCHANGED << holder, orphan, commits, releases >>

(***************************************************************************)
(* SegmentChangeListener.onSegmentClaimed -> WorkflowEngine.claimSegment.    *)
(* loadRunningWorkflows sources the CURRENT durable state (not the segment   *)
(* token position), then removeTerminalAndStartRestoredWorkflowExecutions    *)
(* runs with no segment argument and no live-mode check.                     *)
(***************************************************************************)
Claim(n, s) ==
    /\ holder[s] = NoNode
    /\ LET restored == [w \in Instances |->
                          IF Owner[w] = s /\ store[w] = "active" /\ res[n][w] = "none"
                          THEN "idle" ELSE res[n][w]]
           segLive  == pos[s] >= ReplayBoundary
           liveNext == IF PerSegmentLiveMode /\ segLive
                       THEN [live EXCEPT ![n][s] = TRUE] ELSE live
           gate     == IF ~ClaimRestoreChecksLiveMode THEN TRUE
                       ELSE IF PerSegmentLiveMode THEN liveNext[n][s]
                       ELSE \E t \in Segments : live[n][t]
           scope    == IF ScopedClaimRestore THEN OwnedBy(s) ELSE Instances
           r2       == IF gate THEN StartIdle(restored, scope) ELSE restored
       IN /\ res'     = [res EXCEPT ![n] = r2]
          /\ live'    = liveNext
          /\ actedBy' = [w \in Instances |->
                           IF r2[w] = "running" /\ restored[w] = "idle"
                           THEN actedBy[w] \cup {s} ELSE actedBy[w]]
    /\ holder' = [holder EXCEPT ![s] = n]
    /\ UNCHANGED << pos, store, orphan, spawned, spawnSeen, dueWakes, doneWakes, commits, releases >>

(***************************************************************************)
(* releaseSegment: scoped by ownership, but interrupt() does not join, so    *)
(* an in-flight body survives the release when SyncRelease is FALSE.         *)
(***************************************************************************)
Release(s) ==
    /\ holder[s] # NoNode
    /\ releases < MaxReleases
    /\ LET n == holder[s] IN
       /\ res' = [res EXCEPT ![n] = [w \in Instances |->
                    IF Owner[w] = s THEN "none" ELSE res[n][w]]]
       /\ orphan' = [orphan EXCEPT ![n] = [w \in Instances |->
                    IF Owner[w] = s /\ ~SyncRelease /\ res[n][w] = "running"
                    THEN TRUE ELSE orphan[n][w]]]
    /\ holder'   = [holder EXCEPT ![s] = NoNode]
    /\ releases' = releases + 1
    /\ UNCHANGED << pos, store, live, spawned, spawnSeen, dueWakes, doneWakes, commits, actedBy >>

Drain(n, w) ==
    /\ orphan[n][w]
    /\ orphan' = [orphan EXCEPT ![n][w] = FALSE]
    /\ UNCHANGED << pos, holder, store, res, live, spawned, spawnSeen,
                    dueWakes, doneWakes, commits, actedBy, releases >>

\* The workflow body reaches its terminal state and persists it.
Complete(n, w) ==
    /\ res[n][w] = "running" \/ orphan[n][w]
    /\ store[w] # "none"
    /\ store'   = [store EXCEPT ![w] = "terminal"]
    /\ res'     = [res EXCEPT ![n][w] = IF res[n][w] = "running" THEN "none" ELSE @]
    /\ orphan'  = [orphan EXCEPT ![n][w] = FALSE]
    /\ commits' = [commits EXCEPT ![w] = @ + 1]
    /\ UNCHANGED << pos, holder, live, spawned, spawnSeen,
                    dueWakes, doneWakes, actedBy, releases >>

Next ==
    \/ \E n \in Nodes, s \in Segments : Deliver(n, s) \/ Claim(n, s)
    \/ \E s \in Segments : Release(s)
    \/ \E n \in Nodes, w \in Instances : Drain(n, w) \/ Complete(n, w)

(* Fairness: every held segment keeps consuming its stream; an unheld segment
   is eventually claimed by somebody; a resident body eventually finishes or
   drains.  Release is deliberately NOT fair and is bounded, so churn cannot
   starve progress by itself. *)
Fairness ==
    /\ \A n \in Nodes, s \in Segments : WF_vars(Deliver(n, s))
    /\ \A s \in Segments : WF_vars(\E n \in Nodes : Claim(n, s))
    /\ \A n \in Nodes, w \in Instances : WF_vars(Complete(n, w))
    /\ \A n \in Nodes, w \in Instances : WF_vars(Drain(n, w))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Safety                                                                    *)
(***************************************************************************)
TypeOK ==
    /\ \A s \in Segments  : pos[s] \in 0..Len(Stream)
    /\ \A s \in Segments  : holder[s] \in Nodes \cup {NoNode}
    /\ \A w \in Instances : store[w] \in {"none", "active", "terminal"}
    /\ \A n \in Nodes, w \in Instances :
           res[n][w] \in {"none", "idle", "running"}

\* Only the owning segment ever acts on an instance.
OneInstanceOneOwner == \A w \in Instances : actedBy[w] \subseteq {Owner[w]}

\* A start event consumed by the owning segment creates exactly one execution.
SpawnExactlyOnce == \A w \in Instances : spawned[w] = spawnSeen[w]

\* Every live-region broadcast/routed delivery for an active instance lands once.
WakeExactlyOnce == \A w \in Instances : doneWakes[w] = dueWakes[w]

\* No instance is live on two nodes at the same time.
NoDoubleStart ==
    \A w \in Instances :
        Cardinality({n \in Nodes : res[n][w] = "running" \/ orphan[n][w]}) <= 1

\* No instance executes on a segment that has not caught up.
NoWorkWhileReplaying ==
    \A n \in Nodes, w \in Instances :
        res[n][w] = "running" => pos[Owner[w]] >= ReplayBoundary

AtMostOneCompletion == \A w \in Instances : commits[w] <= 1

(***************************************************************************)
(* Liveness                                                                  *)
(***************************************************************************)
EveryInstanceEventuallyCompletes ==
    \A w \in Instances : (store[w] = "active") ~> (store[w] = "terminal")

MigratedInstanceMakesProgress ==
    \A w \in Instances : (releases > 0 /\ store[w] = "active") ~> (store[w] = "terminal")
=============================================================================
