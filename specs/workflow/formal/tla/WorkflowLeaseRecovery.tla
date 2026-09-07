------------------------- MODULE WorkflowLeaseRecovery -------------------------
(***************************************************************************)
(* Phase 2 DESIGN model of the axon-flow-spec task-leasing + crash-recovery *)
(* protocol.  This is an ABSTRACT model of the protocol described in        *)
(* formal/ARCHITECTURE.md (S2 leasing, S5 replay/recovery, S7 dedup, S11    *)
(* nondeterminism) and the six invariants in formal/INVARIANTS.md.  It is   *)
(* NOT a transliteration of the Java engine.  Scope is deliberately tiny    *)
(* (2 procs, <=3 steps, 1 instance, bounded log/crash count) so TLC finishes *)
(* in seconds.                                                              *)
(*                                                                          *)
(* Durable substrate (survives a crash):                                    *)
(*   log         - ordered sequence of step records <<step, status>>.       *)
(*                 This is the EventStore (ARCHITECTURE S4). Append-only.    *)
(*                                                                          *)
(* Volatile / non-durable (lost on a crash):                                *)
(*   claim       - which proc holds the single-segment processing lease     *)
(*                 (ARCHITECTURE S2).  The processor TokenStore is hardcoded *)
(*                 in-memory (AllEventEventHandlingComponent.java:70), so a  *)
(*                 crash loses the claim and -- unless DURABLE_LEASE -- two  *)
(*                 procs may both hold it (finding F-1).                     *)
(*   vol[p]      - per-proc rebuilt-from-log view (set of step records p     *)
(*                 has observed). Lost on crash, rebuilt on Recover.         *)
(*   pc[p]       - per-proc control: "down" (crashed/not started),          *)
(*                 "ready" (live, rebuilt), used to drive recovery.         *)
(*                                                                          *)
(* Watched counter:                                                         *)
(*   effect[s]   - number of times step s's external side effect has run    *)
(*                 (ExecuteDelegate.java:148 action.apply). INV-6 watches.   *)
(*                                                                          *)
(* Flags (CONSTANTS, toggled per .cfg):                                     *)
(*   DURABLE_LEASE    - TRUE => claim acquisition is mutually exclusive      *)
(*                      (a durable single-writer lease).  FALSE => the       *)
(*                      non-durable in-memory token admits two claimers      *)
(*                      (the F-1 split-brain the code has today).            *)
(*   APPEND_CONDITION - TRUE => the FIXME fix: the COMPLETED append is       *)
(*                      conditional on the log not having changed under the  *)
(*                      writer (optimistic append, ExecuteDelegate FIXME),   *)
(*                      AND the effect is bound to the commit (outbox) so a  *)
(*                      replayed STARTED step does NOT re-run its effect.    *)
(*                      FALSE => today's behaviour: effect re-runs on resume *)
(*                      of a STARTED-but-not-COMPLETED step (finding F-0).   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Procs,            \* set of processes, e.g. {p1, p2}
          Steps,            \* set of step ids, e.g. {1,2,3}  (ordered by <=)
          MaxLogLen,        \* bound on |log| so the state space is finite
          MaxCrashes,       \* bound on total crashes
          DURABLE_LEASE,    \* BOOLEAN
          APPEND_CONDITION  \* BOOLEAN

\* Step record statuses. STARTED = effect window opened; COMPLETED = terminal.
STARTED   == "STARTED"
COMPLETED == "COMPLETED"
Statuses  == {STARTED, COMPLETED}

\* A step record is a function record <<step, status>>.
Rec(s, st) == [step |-> s, status |-> st]

VARIABLES
    log,        \* Seq of step records: the durable committed history
    holds,      \* [Procs -> BOOLEAN] does proc p hold the segment lease?
                \*   Modelled per-proc (not a single holder) because the engine's
                \*   processor TokenStore is hardcoded in-memory & per-process
                \*   (AllEventEventHandlingComponent.java:70): each node has its
                \*   OWN token, so without a durable lease two nodes can each
                \*   believe they hold segment 0 (the F-1 split-brain).
    vol,        \* [Procs -> SUBSET (Steps x Statuses)] volatile rebuilt view
    applied,    \* [Procs -> SUBSET Steps] effects THIS incarnation has run.
                \*   VOLATILE: lost on crash, NOT rebuilt from the log -- this is
                \*   the crux of F-0.  The live owner runs an effect once per
                \*   step (guarded by applied), but on recovery the step is still
                \*   STARTED in the rebuilt log view while `applied` is empty, so
                \*   the engine re-runs action.apply (ExecuteDelegate.java:148):
                \*   the cached-result path only protects COMPLETED steps (S7).
    pc,         \* [Procs -> {"down","ready"}] per-proc liveness/recovery state
    effect,     \* [Steps -> Nat] side-effect run counter per step
    crashes     \* Nat: number of crashes so far (bounded by MaxCrashes)

vars == <<log, holds, vol, applied, pc, effect, crashes>>

\* Is any process other than p currently holding the lease?
OtherHolds(p) == \E q \in Procs : q # p /\ holds[q]

----------------------------------------------------------------------------
\* Helpers over the durable log.

\* Does the durable log contain a record for (s, st)?
InLog(s, st) == \E i \in 1..Len(log) : log[i] = Rec(s, st)

\* Step s is terminal in the durable log iff a COMPLETED record exists.
Committed(s) == InLog(s, COMPLETED)

\* Step s is mid-flight in the durable log: STARTED present, COMPLETED absent.
StartedNotCommitted(s) == InLog(s, STARTED) /\ ~Committed(s)

\* The set of step records derivable purely from the durable log.  This is the
\* deterministic rebuild function used on Recover and asserted by INV-4.
RebuildFromLog ==
    { <<log[i].step, log[i].status>> : i \in 1..Len(log) }

\* All steps terminal in the log (workflow instance is done).
AllStepsCommitted == \A s \in Steps : Committed(s)

----------------------------------------------------------------------------
\* Type invariant.

TypeOK ==
    /\ log \in Seq([step: Steps, status: Statuses])
    /\ Len(log) <= MaxLogLen
    /\ holds \in [Procs -> BOOLEAN]
    /\ vol \in [Procs -> SUBSET (Steps \X Statuses)]
    /\ applied \in [Procs -> SUBSET Steps]
    /\ pc \in [Procs -> {"down", "ready"}]
    /\ effect \in [Steps -> Nat]
    /\ crashes \in 0..MaxCrashes

----------------------------------------------------------------------------
Init ==
    /\ log = << >>
    /\ holds = [p \in Procs |-> FALSE]
    /\ vol = [p \in Procs |-> {}]
    /\ applied = [p \in Procs |-> {}]
    /\ pc = [p \in Procs |-> "down"]
    /\ effect = [s \in Steps |-> 0]
    /\ crashes = 0

----------------------------------------------------------------------------
\* ACTIONS

\* Claim(p): proc p acquires the segment lease and comes up "ready".
\* With DURABLE_LEASE the lease is a durable single-writer mutex: a proc may
\* claim only when no one else holds it.  Without it, the non-durable in-memory
\* token admits a second claimer even while another proc holds the claim
\* (finding F-1: split-brain).  On coming up, p rebuilds its volatile view.
Claim(p) ==
    /\ pc[p] = "down"
    /\ IF DURABLE_LEASE THEN ~OtherHolds(p) ELSE TRUE
    /\ holds' = [holds EXCEPT ![p] = TRUE]
    /\ pc' = [pc EXCEPT ![p] = "ready"]
    /\ vol' = [vol EXCEPT ![p] = RebuildFromLog]
    /\ applied' = [applied EXCEPT ![p] = {}]   \* fresh incarnation: no effects run yet
    /\ UNCHANGED <<log, effect, crashes>>

\* ApplyEffect(p, s): the owner runs step s's external side effect.  Kept
\* SEPARATE from the commit so a crash can interleave between effect and commit
\* (the F-0 window).  Models ExecuteDelegate.java:148 action.apply running while
\* the step is STARTED-and-not-COMPLETED.
\*   - Records STARTED in the durable log (if not already there).
\*   - Increments the effect counter EXCEPT when APPEND_CONDITION is ON and the
\*     step is already STARTED in the log (outbox/append-condition dedup: the
\*     effect is bound to the commit and not re-run on a replayed STARTED step).
\* Guarded so the owner only acts on an un-committed step it is positioned at.
ApplyEffect(p, s) ==
    /\ pc[p] = "ready"
    /\ holds[p]
    /\ ~Committed(s)
    /\ s \notin applied[p]                  \* this incarnation has not run s yet
    \* in-order: every lower step is already committed in the log
    /\ \A t \in Steps : t < s => Committed(t)
    /\ Len(log) < MaxLogLen
    /\ LET resume == InLog(s, STARTED)      \* step already STARTED => post-crash resume
       IN  /\ log' = IF resume THEN log ELSE Append(log, Rec(s, STARTED))
           \* The effect runs.  It increments the external counter EXCEPT on a
           \* resume when the outbox/append-condition fix is ON: then the effect
           \* is bound to the (idempotent) commit and is NOT observed twice.
           /\ effect' = IF (APPEND_CONDITION /\ resume)
                        THEN effect
                        ELSE [effect EXCEPT ![s] = effect[s] + 1]
    /\ applied' = [applied EXCEPT ![p] = applied[p] \cup {s}]
    \* keep the owner's volatile view in sync with what it just wrote/observed
    /\ vol' = [vol EXCEPT ![p] = vol[p] \cup {<<s, STARTED>>}]
    /\ UNCHANGED <<holds, pc, crashes>>

\* CommitCompleted(p, s): append COMPLETED(s) to the durable log -- the
\* at-most-once *recording* point (INV-2).  Guards:
\*   - the step must be STARTED in the durable log (a writer only completes a
\*     step it started) and present in p's OWN volatile view;
\*   - p decides terminal-ness from its OWN in-memory view -- the engine's
\*     publish-side / evolve guards check ITS EventSourcedWorkflowState, not a
\*     shared durable read (AbstractStepExecutor.java:204-212, EventSourced-
\*     WorkflowState.java:195-199).  So a STALE split-brain writer that has not
\*     seen the other owner's COMPLETED still believes the step is open.
\*   - APPEND_CONDITION (the FIXME fix, ExecuteDelegate.java:163...) adds an
\*     optimistic guard: append only if the writer's view still matches the
\*     durable log AND the step is not already terminal in the durable log.
\*     This rejects the stale writer, so the duplicate COMPLETED cannot land.
\* => With the fix OFF and INV-1 broken (two owners), two stale writers can each
\*    append COMPLETED(s): AtMostOnceRecording is violable (relates to F-1).
\*    With the fix ON, the second append is rejected.
CommitCompleted(p, s) ==
    /\ pc[p] = "ready"
    /\ holds[p]
    /\ InLog(s, STARTED)
    /\ <<s, STARTED>> \in vol[p]
    /\ <<s, COMPLETED>> \notin vol[p]              \* p's OWN view: step still open
    /\ Len(log) < MaxLogLen
    /\ IF APPEND_CONDITION
       THEN (vol[p] = RebuildFromLog /\ ~Committed(s))   \* optimistic append guard
       ELSE TRUE
    /\ log' = Append(log, Rec(s, COMPLETED))
    /\ vol' = [vol EXCEPT ![p] = vol[p] \cup {<<s, COMPLETED>>}]
    /\ UNCHANGED <<holds, applied, effect, pc, crashes>>

\* Crash(p): process loss.  Drops p's volatile state and (non-durable) claim;
\* the committed log survives.  Bounded by MaxCrashes.
Crash(p) ==
    /\ pc[p] = "ready"
    /\ crashes < MaxCrashes
    /\ pc' = [pc EXCEPT ![p] = "down"]
    /\ vol' = [vol EXCEPT ![p] = {}]
    /\ applied' = [applied EXCEPT ![p] = {}]  \* volatile: effect-applied set is lost
    /\ holds' = [holds EXCEPT ![p] = FALSE]   \* non-durable claim lost on crash
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<log, effect>>

\* Recover(p): a down proc rebuilds volatile state by replaying the durable log
\* from the safe point (modelled as "from the start of the committed log";
\* faithful to ADR-004 -- recovery never loses committed events, may replay
\* more than necessary but never less).  A step COMPLETED in the log returns a
\* cached result (no effect re-run).  A step left STARTED-but-not-COMPLETED will
\* be re-driven by ApplyEffect after recovery -- THIS is what makes INV-6 fail
\* today (the effect re-runs).  Recover is exactly a re-Claim + rebuild.
Recover(p) ==
    /\ pc[p] = "down"
    /\ Len(log) > 0                       \* there is committed history to replay
    /\ IF DURABLE_LEASE THEN ~OtherHolds(p) ELSE TRUE
    /\ holds' = [holds EXCEPT ![p] = TRUE]
    /\ pc' = [pc EXCEPT ![p] = "ready"]
    /\ vol' = [vol EXCEPT ![p] = RebuildFromLog]   \* deterministic rebuild
    \* applied is NOT rebuilt from the log: a step left STARTED is re-applied.
    /\ applied' = [applied EXCEPT ![p] = {}]
    /\ UNCHANGED <<log, effect, crashes>>

----------------------------------------------------------------------------
\* Progress action used for liveness (EventuallyTerminates).  It is just the
\* union of the forward-progress steps; weak fairness is asserted on it in the
\* liveness spec so the instance is driven to terminal.
Progress ==
    \/ \E p \in Procs, s \in Steps : ApplyEffect(p, s)
    \/ \E p \in Procs, s \in Steps : CommitCompleted(p, s)

\* Terminal idle: once every step is COMPLETED the instance is done and simply
\* stutters.  This keeps behaviours infinite (so TLC does not report the
\* legitimate terminal state as a deadlock) without affecting any safety
\* invariant -- it leaves all variables unchanged.
Done ==
    /\ AllStepsCommitted
    /\ UNCHANGED vars

Next ==
    \/ \E p \in Procs : Claim(p)
    \/ \E p \in Procs : Recover(p)
    \/ \E p \in Procs : Crash(p)
    \/ \E p \in Procs, s \in Steps : ApplyEffect(p, s)
    \/ \E p \in Procs, s \in Steps : CommitCompleted(p, s)
    \/ Done

Spec == Init /\ [][Next]_vars

\* Liveness spec: weak fairness on Claim/Recover (so a down proc comes back up)
\* and on Progress (so a live owner keeps driving steps).  Crashes are bounded
\* by MaxCrashes, so after the last crash the fair actions carry the instance to
\* terminal.  (We do NOT make Crash fair -- it is adversarial, bounded.)
FairSpec ==
    /\ Spec
    /\ \A p \in Procs : WF_vars(Claim(p))
    /\ \A p \in Procs : WF_vars(Recover(p))
    /\ WF_vars(Progress)

----------------------------------------------------------------------------
\* INVARIANTS  (names match formal/INVARIANTS.md MachineName verbatim)
----------------------------------------------------------------------------

\* INV-1 AtMostOneOwner (Safety): at most one proc holds the processing claim
\* at any instant.  Two live procs both holding the claim is split-brain (F-1).
AtMostOneOwner ==
    Cardinality({ p \in Procs : holds[p] /\ pc[p] = "ready" }) <= 1

\* INV-2 AtMostOnceRecording (Safety): a given step reaches a terminal
\* (COMPLETED) record at most once in the durable log.
AtMostOnceRecording ==
    \A s \in Steps :
        Cardinality({ i \in 1..Len(log) : log[i] = Rec(s, COMPLETED) }) <= 1

\* INV-3 CommittedHistorySurvivesCrash (Safety): a crash never shrinks or
\* reorders the durable committed history.  Encoded as a step-relation property
\* (an action invariant over Crash): the post-crash log equals the pre-crash log
\* (Crash leaves `log` UNCHANGED), so every committed COMPLETED prefix survives.
CommittedHistorySurvivesCrash ==
    [][ (\E p \in Procs : Crash(p)) => (log' = log) ]_vars

\* INV-4 DeterministicReplay (Safety): the rebuilt state is a pure function of
\* the durable committed log.  Encoded as two checks that together say "replay
\* never diverges from the log and the committed prefix is never rewritten":
\*   (a) every record derivable from the log is present in each live proc's view
\*       (recovery/rebuild loses nothing -- RebuildFromLog is reproduced); and
\*   (b) the set of terminal (COMPLETED) records in any live proc's view is
\*       exactly the set of COMPLETED records in the durable log -- no proc
\*       fabricates, drops, or rewrites a committed terminal record.  Because a
\*       COMPLETED record enters a proc's view only atomically with its log
\*       append (CommitCompleted), the two always agree; were a stale split-brain
\*       writer able to diverge the committed history, this would catch it.
CommittedSet == { <<s, COMPLETED>> : s \in { st \in Steps : Committed(st) } }

\* Quantified over the proc actually driving the instance (the claim holder) --
\* that is the proc that has rebuilt from the log and is replaying it.  Under a
\* non-durable lease (DURABLE_LEASE=FALSE) two procs may hold the claim at once;
\* if either diverged from the committed history this would catch it.
DeterministicReplay ==
    \A p \in Procs :
        (pc[p] = "ready" /\ holds[p]) =>
            /\ RebuildFromLog \subseteq vol[p]
            /\ { r \in vol[p] : r[2] = COMPLETED } = CommittedSet

\* INV-6 EffectAtMostOnce (Safety): each step's external effect runs at most
\* once across the whole lifetime, including across crashes/replays.  This is
\* the F-0 gap: with APPEND_CONDITION off, a crash in the STARTED->COMMIT window
\* lets Recover + ApplyEffect run the effect twice.
EffectAtMostOnce ==
    \A s \in Steps : effect[s] <= 1

\* INV-5 EventuallyTerminates (Liveness): every started instance eventually
\* reaches terminal (all steps COMPLETED in the durable log).  Temporal
\* property; checked under FairSpec.  Crashes are bounded (MaxCrashes), so the
\* fair Recover/Progress actions eventually drive every step to COMPLETED.
EventuallyTerminates == <>(AllStepsCommitted)

===============================================================================
