---------------------------- MODULE Holdback ----------------------------
(***************************************************************************)
(* Token advancement and the holdback barrier in the workflow engine.      *)
(*                                                                         *)
(* Claim under test:                                                       *)
(*   "The stored segment token never advances past an event whose effects  *)
(*    are not yet durable, therefore a rebalance always redelivers any     *)
(*    event whose wake was not completed."                                 *)
(*                                                                         *)
(* Modelled from ewf-shard-dst @ dda2df88:                                 *)
(*   WorkflowEngine.handle              -> deliver, then requestCheckpoint *)
(*   SimpleWorkflowExecution.onEvent    -> live: enqueue match task        *)
(*                                         not running: evolve only (drop) *)
(*   WaitForDelegate.eventReceived      -> match enqueues completion task  *)
(*   WorkflowExecutionCheckpointSupport -> running /\ (active \/ queued    *)
(*                                          \/ intentQueued)              *)
(*   WorkflowEngineCheckpointingSupport -> barrier + RECHECK RECURSION     *)
(*   CheckpointingProgressStrategy      -> drain toward lastConsumedToken  *)
(*   Coordinator.abortWorkPackage       -> drain, store, release, then     *)
(*                                         segmentChangeListener release   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Segments,             \* set of segment ids
    Nodes,                \* set of nodes
    InstPerSeg,           \* workflow instances owned by each segment
    MaxPos,               \* stream length
    MaxHandovers,         \* bound on crash/release/shutdown events

    (* ---- knobs: each models a real reading of the code, or a mutant ---- *)
    DropOnNotRunning,     \* TRUE  = SimpleWorkflowExecution:399 as written
                          \*         (running==false evolves state, never
                          \*          evaluates wait conditions)
                          \* FALSE = hypothetical fix: match anyway
    AllowShortClaim,      \* TRUE  = a claim may land while isReplaying(seg)
                          \*         holds, so bodies are materialized but
                          \*         not started (WorkflowEngine:297)
    HoldbackEnabled,      \* FALSE = MUTANT: no barrier at all
    BarrierRecheck,       \* FALSE = MUTANT: single-round barrier, no
                          \*         recursion at CheckpointingSupport:139
    ShutdownClearsFirst,  \* TRUE  = WorkflowEngine.shutdown() clears the
                          \*         repository before the processor drain
                          \*         (both are Phase.INBOUND_EVENT_CONNECTORS
                          \*          and run concurrently under allOf)
    HoldbackRequiresRunning, \* TRUE  = WorkflowExecutionCheckpointSupport as
                          \*         written: !host.isExecutable() short-circuits
                          \*         hasPendingCheckpointWork to FALSE, and
                          \*         isExecutable() == running
                          \* FALSE = candidate fix: a materialized execution
                          \*         with queued work holds the token back even
                          \*         before its body is started
    RestoreIgnoresCompletion, \* TRUE = MUTANT: a restored body re-registers a
                          \*        wait whose Completed event is already
                          \*        durable, so a redelivery completes it twice
    PartialDrainStore,    \* TRUE  = a node shutdown may store a partially
                          \*         drained position (lifecycle phase
                          \*         timeout / reconcile lowerBound fallback)
    NonTerminalExit,      \* how a body that stops without a terminal status
                          \* (drift pause, recoverable exception, failed
                          \* append) leaves the engine:
                          \* "none"   = not modelled (every C* / M* arm)
                          \* "finish" = the execution is removed as if it
                          \*            finished (termination handler)
                          \* "pause"  = the execution stays registered with
                          \*            its driver stopped (stopRuntimeForRecovery)
    PausedQueuesWakes     \* TRUE  = candidate fix: a paused execution queues
                          \*         a matching wake and counts towards the
                          \*         barrier, like a restored-not-started one
                          \* FALSE = code as written: the stopped-for-recovery
                          \*         branch only evolves its own events, and
                          \*         hasUnsafeCheckpointWork is FALSE

(* An instance is identified by <<owning segment, ordinal>>: the instance
   partitioning (SegmentedWorkflowRouting.shouldHandle) is a fixed function of
   the workflow id, so it is a static mapping here. *)
Instances == Segments \X (1..InstPerSeg)
SegOf     == [i \in Instances |-> i[1]]

(* Sentinels. Segments and Nodes are positive naturals, so 0 / <<0,0>> are
   outside every domain and comparisons stay type-consistent for TLC. *)
NoInst  == <<0, 0>>
NoNode  == 0
NoTask  == [k |-> "none", pos |-> 0]
NoCp    == [target |-> 0, mode |-> "none", cb |-> FALSE]

Match(p)    == [k |-> "match",    pos |-> p]
Complete(p) == [k |-> "complete", pos |-> p]
Intent      == [k |-> "intent",   pos |-> 0]

VARIABLES
    storedToken,   \* [Segments -> 0..MaxPos] durable token
    lastConsumed,  \* [Segments -> 0..MaxPos] position delivered up to
    owner,         \* [Segments -> Nodes \cup {NoNode}]
    replaying,     \* [Segments -> BOOLEAN] segment behind, bodies not started
    cp,            \* [Segments -> cp record] in-flight checkpoint advance
    present,       \* [Instances -> BOOLEAN] materialized in the repository
    running,       \* [Instances -> BOOLEAN] body started (the `running` flag)
    waiting,       \* [Instances -> BOOLEAN] wait condition registered
    completed,     \* [Instances -> BOOLEAN] wait completion durably published
    ncomplete,     \* [Instances -> Nat] how many times it was published
    dropped,       \* [Instances -> BOOLEAN] a wake hit the running==false path
    queue,         \* [Instances -> Seq(task)] the per-instance task queue
    current,       \* [Instances -> task] task currently executing (taskActive)
    intentQueued,  \* [Instances -> BOOLEAN] CheckpointIntent in flight
    evTarget,      \* [1..MaxPos -> Instances \cup {NoInst}] the stream
    handovers      \* Nat, bounded by MaxHandovers

vars == <<storedToken, lastConsumed, owner, replaying, cp, present, running,
          waiting, completed, ncomplete, dropped, queue, current,
          intentQueued, evTarget, handovers>>

Positions == 1..MaxPos
Tokens    == 0..MaxPos

Owned(s)  == {i \in Instances : SegOf[i] = s}
Max(a, b) == IF a > b THEN a ELSE b

(* The position of the event instance i is waiting for. Init guarantees it
   exists and is unique. *)
WaitPos(i) == CHOOSE p \in Positions : evTarget[p] = i

(* ------------------------------------------------------------------ *)
(* WorkflowExecutionCheckpointSupport.hasPendingCheckpointWork, lifted *)
(* to the segment by WorkflowEngine.pendingCheckpointWorkOf.           *)
(* Note !host.isExecutable() -> FALSE : a materialized but NOT running *)
(* execution holds nothing back at all.                                *)
(* ------------------------------------------------------------------ *)
(* A paused execution: registered, driver stopped, and not because its
   segment is still replaying. Reachable only through BodyStops. *)
Paused(i) == present[i] /\ ~running[i] /\ ~replaying[SegOf[i]]

PendingWorkOf(i) ==
    /\ present[i]
    /\ \/ running[i]
       \/ ~HoldbackRequiresRunning /\ ~Paused(i)
       \/ Paused(i) /\ PausedQueuesWakes
    /\ (current[i] /= NoTask \/ queue[i] /= <<>> \/ intentQueued[i])

HasPendingWork(s) == \E i \in Owned(s) : PendingWorkOf(i)

(* ------------------------------------------------------------------ *)
(* Dropping the instances of a segment: crash, release, shutdown.      *)
(* Queued completion tasks are discarded with the queue.               *)
(* ------------------------------------------------------------------ *)
DropSegInstances(s) ==
    /\ present'      = [i \in Instances |-> IF i \in Owned(s) THEN FALSE ELSE present[i]]
    /\ running'      = [i \in Instances |-> IF i \in Owned(s) THEN FALSE ELSE running[i]]
    /\ queue'        = [i \in Instances |-> IF i \in Owned(s) THEN <<>>  ELSE queue[i]]
    /\ current'      = [i \in Instances |-> IF i \in Owned(s) THEN NoTask ELSE current[i]]
    /\ intentQueued' = [i \in Instances |-> IF i \in Owned(s) THEN FALSE ELSE intentQueued[i]]

KeepInstances ==
    UNCHANGED <<present, running, queue, current, intentQueued>>

(***************************************************************************)
(* Init                                                                    *)
(***************************************************************************)
(* The whole stream is published up front: every instance has exactly one
   wake event somewhere in 1..MaxPos, remaining positions are filler events
   that wake nobody but still move lastConsumed and offer checkpoint points. *)
ValidStream(f) ==
    \A i \in Instances : Cardinality({p \in Positions : f[p] = i}) = 1

Init ==
    /\ evTarget \in {f \in [Positions -> Instances \cup {NoInst}] : ValidStream(f)}
    /\ storedToken  = [s \in Segments |-> 0]
    /\ lastConsumed = [s \in Segments |-> 0]
    /\ owner        = [s \in Segments |-> NoNode]
    /\ replaying    = [s \in Segments |-> FALSE]
    /\ cp           = [s \in Segments |-> NoCp]
    /\ present      = [i \in Instances |-> FALSE]
    /\ running      = [i \in Instances |-> FALSE]
    /\ waiting      = [i \in Instances |-> FALSE]
    /\ completed    = [i \in Instances |-> FALSE]
    /\ ncomplete    = [i \in Instances |-> 0]
    /\ dropped      = [i \in Instances |-> FALSE]
    /\ queue        = [i \in Instances |-> <<>>]
    /\ current      = [i \in Instances |-> NoTask]
    /\ intentQueued = [i \in Instances |-> FALSE]
    /\ handovers    = 0

(***************************************************************************)
(* Claim: WorkflowEngine.claimSegment                                      *)
(*   loadRunningWorkflows -> materialize from durable state                 *)
(*   isReplaying(segment) -> materialize only, do NOT start bodies          *)
(*   reading restarts at the stored token                                   *)
(***************************************************************************)
Claim(s, n) ==
    /\ owner[s] = NoNode
    /\ cp[s].mode = "none"
    /\ \E rep \in (IF AllowShortClaim THEN {TRUE, FALSE} ELSE {FALSE}) :
         /\ replaying' = [replaying EXCEPT ![s] = rep]
         /\ running'   = [i \in Instances |-> IF i \in Owned(s) THEN ~rep ELSE running[i]]
    /\ owner'        = [owner EXCEPT ![s] = n]
    /\ lastConsumed' = [lastConsumed EXCEPT ![s] = storedToken[s]]
    /\ present'      = [i \in Instances |-> IF i \in Owned(s) THEN TRUE ELSE present[i]]
    (* the body re-registers its wait condition unless the wait already
       completed durably (the Completed event is in the instance's stream) *)
    /\ waiting'      = [i \in Instances |->
                          IF i \in Owned(s)
                          THEN (RestoreIgnoresCompletion \/ ~completed[i])
                          ELSE waiting[i]]
    /\ queue'        = [i \in Instances |-> IF i \in Owned(s) THEN <<>>  ELSE queue[i]]
    /\ current'      = [i \in Instances |-> IF i \in Owned(s) THEN NoTask ELSE current[i]]
    /\ intentQueued' = [i \in Instances |-> IF i \in Owned(s) THEN FALSE ELSE intentQueued[i]]
    /\ UNCHANGED <<storedToken, cp, completed, ncomplete, dropped, evTarget, handovers>>

(* removeTerminalAndStartRestoredWorkflowExecutions, after replay catch-up *)
CatchUp(s) ==
    /\ owner[s] /= NoNode
    /\ replaying[s]
    /\ replaying' = [replaying EXCEPT ![s] = FALSE]
    /\ running'   = [i \in Instances |-> IF i \in Owned(s) /\ present[i] THEN TRUE ELSE running[i]]
    /\ UNCHANGED <<storedToken, lastConsumed, owner, cp, present, waiting, completed,
                   ncomplete, dropped, queue, current, intentQueued, evTarget, handovers>>

(***************************************************************************)
(* Deliver: WorkflowEngine.handle                                           *)
(*   execution.onEvent(...)   FIRST                                         *)
(*   requestCheckpoint(...)   AFTER  (modelled as enabling CheckpointAttempt)*)
(***************************************************************************)
Deliver(s) ==
    /\ owner[s] /= NoNode
    /\ lastConsumed[s] < MaxPos
    /\ LET p == lastConsumed[s] + 1
           tgt == evTarget[p]
           (* live mode: enqueue the match; not running: the drop branch *)
           Enq(i) == /\ present[i]
                     /\ \/ running[i]
                        \/ ~DropOnNotRunning /\ ~Paused(i)
                        \/ Paused(i) /\ PausedQueuesWakes
       IN
       /\ queue' = [i \in Instances |->
                      IF i \in Owned(s) /\ Enq(i)
                      THEN Append(queue[i], Match(p))
                      ELSE queue[i]]
       /\ dropped' = [i \in Instances |->
                      IF /\ i \in Owned(s) /\ present[i] /\ ~Enq(i)
                         /\ waiting[i] /\ tgt = i
                      THEN TRUE ELSE dropped[i]]
       /\ lastConsumed' = [lastConsumed EXCEPT ![s] = p]
    /\ UNCHANGED <<storedToken, owner, replaying, cp, present, running, waiting,
                   completed, ncomplete, current, intentQueued, evTarget, handovers>>

(***************************************************************************)
(* The instance task queue. Split into Start/Finish so that taskActive is   *)
(* observable: a checkpoint attempt can land while a task runs but has not  *)
(* yet appended its follow-up.                                              *)
(***************************************************************************)
(* Only a started body drains the queue: awaitStateChange / taskQueue.take()
   is driven by the workflow driver thread, which does not exist until
   execute() has been called. A materialized-but-not-started execution
   therefore accumulates tasks that nobody runs. *)
TaskStart(i) ==
    /\ present[i]
    /\ running[i]
    /\ current[i] = NoTask
    /\ queue[i] /= <<>>
    /\ current' = [current EXCEPT ![i] = Head(queue[i])]
    /\ queue'   = [queue   EXCEPT ![i] = Tail(queue[i])]
    /\ UNCHANGED <<storedToken, lastConsumed, owner, replaying, cp, present, running,
                   waiting, completed, ncomplete, dropped, intentQueued, evTarget, handovers>>

TaskFinish(i) ==
    /\ present[i]
    /\ current[i] /= NoTask
    /\ LET t == current[i] IN
       /\ current' = [current EXCEPT ![i] = NoTask]
       /\ CASE t.k = "match" ->
                 (* EventWaitConditions.evaluateAndApply -> eventReceived ->
                    appendTask(publish completion) : a SECOND task *)
                 IF waiting[i] /\ evTarget[t.pos] = i
                 THEN /\ waiting' = [waiting EXCEPT ![i] = FALSE]
                      /\ queue'   = [queue   EXCEPT ![i] = Append(@, Complete(t.pos))]
                      /\ UNCHANGED <<completed, ncomplete, intentQueued, cp>>
                 ELSE UNCHANGED <<waiting, queue, completed, ncomplete, intentQueued, cp>>
            [] t.k = "complete" ->
                 /\ completed' = [completed EXCEPT ![i] = TRUE]
                 /\ ncomplete' = [ncomplete EXCEPT ![i] = @ + 1]
                 /\ UNCHANGED <<waiting, queue, intentQueued, cp>>
            [] t.k = "intent" ->
                 (* CheckpointIntent.accept -> clear flag, arm the callback *)
                 /\ intentQueued' = [intentQueued EXCEPT ![i] = FALSE]
                 /\ cp' = [cp EXCEPT ![SegOf[i]].cb = TRUE]
                 /\ UNCHANGED <<waiting, queue, completed, ncomplete>>
    /\ UNCHANGED <<storedToken, lastConsumed, owner, replaying, present, running,
                   dropped, evTarget, handovers>>

(***************************************************************************)
(* A body stops without a terminal status while it waits, with nothing     *)
(* queued: SimpleWorkflowExecution's driver finally block on a non-terminal *)
(* exit. The engine is not shutting down and the segment stays claimed.     *)
(* Consumes the disruption budget, like a handover.                         *)
(***************************************************************************)
BodyStops(i) ==
    /\ NonTerminalExit /= "none"
    /\ present[i] /\ running[i] /\ waiting[i] /\ ~completed[i]
    /\ current[i] = NoTask /\ queue[i] = <<>> /\ ~intentQueued[i]
    /\ handovers < MaxHandovers
    /\ handovers' = handovers + 1
    /\ running'   = [running EXCEPT ![i] = FALSE]
    /\ present'   = [present EXCEPT ![i] = (NonTerminalExit = "pause")]
    /\ UNCHANGED <<storedToken, lastConsumed, owner, replaying, cp, waiting, completed,
                   ncomplete, dropped, queue, current, intentQueued, evTarget>>

(***************************************************************************)
(* The barrier: WorkflowEngineCheckpointingSupport.onCheckpointAdvanced     *)
(***************************************************************************)
(* Effects of finishing a checkpoint advance toward t in the given mode.    *)
StoreAt(s, t) == storedToken' = [storedToken EXCEPT ![s] = Max(@, t)]

FinishAdvance(s, t, mode) ==
    /\ StoreAt(s, t)
    /\ cp' = [cp EXCEPT ![s] = NoCp]
    /\ IF mode \in {"release", "shutdown"}
       THEN /\ owner'     = [owner     EXCEPT ![s] = NoNode]
            /\ replaying' = [replaying EXCEPT ![s] = FALSE]
            /\ DropSegInstances(s)
       ELSE /\ UNCHANGED <<owner, replaying>>
            /\ KeepInstances

(* Deferring: append a CheckpointIntent to every pending execution of s.
   WorkflowEngine.scheduleCheckpointIntent *)
DeferAdvance(s, t, mode) ==
    /\ queue' = [i \in Instances |->
                   IF i \in Owned(s) /\ PendingWorkOf(i) /\ ~intentQueued[i]
                   THEN Append(queue[i], Intent) ELSE queue[i]]
    /\ intentQueued' = [i \in Instances |->
                   IF i \in Owned(s) /\ PendingWorkOf(i) THEN TRUE ELSE intentQueued[i]]
    /\ cp' = [cp EXCEPT ![s] = [target |-> t, mode |-> mode, cb |-> FALSE]]
    /\ UNCHANGED <<storedToken, owner, replaying, present, running>>

Advance(s, t, mode) ==
    IF HoldbackEnabled /\ HasPendingWork(s)
    THEN DeferAdvance(s, t, mode)
    ELSE FinishAdvance(s, t, mode)

AdvanceUnchanged ==
    UNCHANGED <<lastConsumed, waiting, completed, ncomplete, dropped, current,
                evTarget>>

(* Periodic checkpointing at an arbitrary point: the free action. Covers
   auto mode (batch end == lastConsumed) and any earlier requested position. *)
CheckpointAttempt(s) ==
    /\ owner[s] /= NoNode
    /\ cp[s].mode = "none"
    /\ storedToken[s] < lastConsumed[s]
    /\ \E t \in (storedToken[s] + 1)..lastConsumed[s] :
         Advance(s, t, "periodic")
    /\ AdvanceUnchanged
    /\ UNCHANGED handovers

(* The barrier callback fired: re-check. This recursion is the whole
   soundness argument -- the completion task was appended BEHIND the intent. *)
Recheck(s) ==
    /\ cp[s].mode /= "none"
    /\ cp[s].cb
    /\ IF BarrierRecheck
       THEN Advance(s, cp[s].target, cp[s].mode)
       ELSE FinishAdvance(s, cp[s].target, cp[s].mode)   \* MUTANT
    /\ AdvanceUnchanged
    /\ UNCHANGED handovers

(***************************************************************************)
(* Mode 1: segment release (rebalance, node stays alive).                   *)
(* Coordinator.abortWorkPackage: drain+store, releaseClaim, THEN the engine *)
(* segmentChangeListener.onSegmentReleased. So instances are still in the   *)
(* repository during the drain.                                             *)
(***************************************************************************)
ReleaseSegment(s) ==
    /\ owner[s] /= NoNode
    /\ cp[s].mode = "none"
    /\ handovers < MaxHandovers
    /\ handovers' = handovers + 1
    /\ Advance(s, lastConsumed[s], "release")
    /\ AdvanceUnchanged

(***************************************************************************)
(* Mode 2: crash. No drain at all; the stored token is wherever the last    *)
(* periodic checkpoint left it.                                             *)
(***************************************************************************)
Crash(n) ==
    /\ \E s \in Segments : owner[s] = n
    /\ handovers < MaxHandovers
    /\ handovers' = handovers + 1
    /\ LET dead == {s \in Segments : owner[s] = n} IN
       /\ owner'        = [s \in Segments |-> IF s \in dead THEN NoNode ELSE owner[s]]
       /\ replaying'    = [s \in Segments |-> IF s \in dead THEN FALSE ELSE replaying[s]]
       /\ cp'           = [s \in Segments |-> IF s \in dead THEN NoCp  ELSE cp[s]]
       /\ present'      = [i \in Instances |-> IF SegOf[i] \in dead THEN FALSE ELSE present[i]]
       /\ running'      = [i \in Instances |-> IF SegOf[i] \in dead THEN FALSE ELSE running[i]]
       /\ queue'        = [i \in Instances |-> IF SegOf[i] \in dead THEN <<>> ELSE queue[i]]
       /\ current'      = [i \in Instances |-> IF SegOf[i] \in dead THEN NoTask ELSE current[i]]
       /\ intentQueued' = [i \in Instances |-> IF SegOf[i] \in dead THEN FALSE ELSE intentQueued[i]]
    /\ UNCHANGED <<storedToken, lastConsumed, waiting, completed, ncomplete,
                   dropped, evTarget>>

(***************************************************************************)
(* Mode 3: graceful node shutdown (SIGTERM / context.close()).              *)
(* WorkflowEngine::shutdown and PooledStreamingEventProcessor::shutdown are *)
(* BOTH registered at Phase.INBOUND_EVENT_CONNECTORS, and same-phase        *)
(* handlers are launched together and joined with allOf                     *)
(* (DefaultAxonApplication.invokeLifecycleHandlers:222-227). They race.     *)
(*                                                                          *)
(* ShutdownClearsFirst = TRUE models WorkflowEngine.shutdown() winning:     *)
(* the repository is cleared, so the drain that follows finds nothing to    *)
(* hold it back and stores lastConsumed outright.                           *)
(***************************************************************************)
ShutdownNode(n) ==
    /\ \E s \in Segments : owner[s] = n /\ cp[s].mode = "none"
    /\ handovers < MaxHandovers
    /\ handovers' = handovers + 1
    /\ \E s \in Segments :
         /\ owner[s] = n
         /\ cp[s].mode = "none"
         /\ IF ShutdownClearsFirst
            THEN (* repository cleared first: holdback is blind *)
                 /\ StoreAt(s, lastConsumed[s])
                 /\ cp'        = [cp    EXCEPT ![s] = NoCp]
                 /\ owner'     = [owner EXCEPT ![s] = NoNode]
                 /\ replaying' = [replaying EXCEPT ![s] = FALSE]
                 /\ DropSegInstances(s)
            ELSE (* drain first, then clear: same shape as a release *)
                 Advance(s, lastConsumed[s], "shutdown")
    /\ AdvanceUnchanged

(* The drain races process exit: the lifecycle phase times out, or reconcile
   falls back to the lowest reported safe token. The store can therefore land
   anywhere between the last periodic checkpoint and the full drain target. *)
PartialShutdownStore(s) ==
    /\ PartialDrainStore
    /\ cp[s].mode = "shutdown"
    /\ storedToken[s] < cp[s].target
    /\ \E t \in (storedToken[s] + 1)..cp[s].target :
         /\ StoreAt(s, t)
         /\ cp'        = [cp    EXCEPT ![s] = NoCp]
         /\ owner'     = [owner EXCEPT ![s] = NoNode]
         /\ replaying' = [replaying EXCEPT ![s] = FALSE]
         /\ DropSegInstances(s)
    /\ AdvanceUnchanged
    /\ UNCHANGED handovers

(***************************************************************************)
Next ==
    \/ \E s \in Segments, n \in Nodes : Claim(s, n)
    \/ \E s \in Segments : CatchUp(s)
    \/ \E s \in Segments : Deliver(s)
    \/ \E i \in Instances : TaskStart(i)
    \/ \E i \in Instances : TaskFinish(i)
    \/ \E i \in Instances : BodyStops(i)
    \/ \E s \in Segments : CheckpointAttempt(s)
    \/ \E s \in Segments : Recheck(s)
    \/ \E s \in Segments : ReleaseSegment(s)
    \/ \E s \in Segments : PartialShutdownStore(s)
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : ShutdownNode(n)

(* Fairness: the progress actions are fair, the disruptions are not. The
   disruptions are additionally bounded by MaxHandovers, so after finitely
   many of them the system is left alone and must make progress. *)
Fairness ==
    /\ \A i \in Instances : WF_vars(TaskStart(i)) /\ WF_vars(TaskFinish(i))
    /\ \A s \in Segments  : WF_vars(Deliver(s))
    /\ \A s \in Segments  : WF_vars(CatchUp(s))
    /\ \A s \in Segments  : WF_vars(Recheck(s))
    /\ \A s \in Segments  : SF_vars(\E n \in Nodes : Claim(s, n))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

(* THE CORE CLAIM. If the stored token of a segment covers the position of
   the wake event of an instance it owns, then that instance's wait
   completion must already be durable. Otherwise a crash at this instant
   loses the wake permanently: nothing will ever redeliver that position. *)
TokenNeverPassesUnappliedEvent ==
    \A i \in Instances :
        storedToken[SegOf[i]] >= WaitPos(i) => completed[i]

(* An uncompleted wake must always still be recoverable: either the segment
   has not consumed past it yet (so a re-read redelivers it), or it is in
   flight inside the owning instance (queued or executing). *)
InFlight(i) ==
    \/ current[i].k \in {"match", "complete"}
    \/ \E x \in 1..Len(queue[i]) : queue[i][x].k \in {"match", "complete"}

(* lastConsumed is WorkPackage state and dies with the claim; an unowned
   segment resumes from its stored token. *)
EffectiveConsumed(s) == IF owner[s] = NoNode THEN storedToken[s] ELSE lastConsumed[s]

WakeSurvivesHandover ==
    \A i \in Instances :
        ~completed[i] =>
            \/ EffectiveConsumed(SegOf[i]) < WaitPos(i)
            \/ InFlight(i)

(* Redelivery after a handover must not complete the same wait twice. *)
NoDuplicateCompletion == \A i \in Instances : ncomplete[i] <= 1

(* The running==false branch never silently swallows a wake. *)
NoSilentDrop == \A i \in Instances : ~dropped[i]

(* Liveness. *)
EveryWaitingInstanceEventuallyWakes ==
    \A i \in Instances : <>completed[i]

(***************************************************************************)
TypeOK ==
    /\ storedToken  \in [Segments -> Tokens]
    /\ lastConsumed \in [Segments -> Tokens]
    /\ owner        \in [Segments -> Nodes \cup {NoNode}]
    /\ handovers    \in 0..MaxHandovers

=============================================================================
