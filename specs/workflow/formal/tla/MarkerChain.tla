----------------------------- MODULE MarkerChain -----------------------------
(***************************************************************************)
(* Design-level model of the DCB append-condition fencing introduced in    *)
(* "Introduce DCB append conditions" (WorkflowAppendConditions /           *)
(* ConsistencyMarkerSupport): every event of one workflow instance is      *)
(* appended under criteria "workflowId=X" checked from a per-writer        *)
(* ConsistencyMarker.                                                      *)
(*                                                                         *)
(* Protocol modelled:                                                      *)
(*  - A fresh spawn has no marker; its first append (Started) is checked   *)
(*    from ORIGIN: accepted iff NO event for the instance exists yet.      *)
(*  - Restore (segment claim) seeds the marker from the head of the store  *)
(*    at claim-sourcing time and rebuilds the writer's program position    *)
(*    (intent) by replaying the store.                                     *)
(*  - Store semantics (single instance, so every record matches the        *)
(*    criteria): an append with marker M commits iff no record exists at   *)
(*    a position > M; on commit the record takes the next position and     *)
(*    the writer's marker advances to it. A REJECTED append never          *)
(*    advances the marker; the rejected writer is interrupted and          *)
(*    publishes nothing further (in particular no terminal event).         *)
(*  - Within one writer appends are strictly serialized: each action is    *)
(*    one atomic append handed the marker the previous successful append   *)
(*    wrote at (ConsistencyMarkerSupport.appendSequentially).              *)
(*  - Ownership loss is NOT modelled as a writer-visible event -- that is  *)
(*    the point. A stale writer simply keeps executing its next intent on  *)
(*    its old marker; only the store's condition check can stop it.        *)
(*                                                                         *)
(* Toggles:                                                                *)
(*  - ConditionalAppends = FALSE removes the fencing (every append         *)
(*    commits unconditionally): OneStartPerInstance and                    *)
(*    AtMostOnceRecording become violable (MC_markerchain_broken.cfg),     *)
(*    proving the model is refutable.                                      *)
(*  - MarkerRegressEnabled = TRUE adds MarkerRegress, the lost-update      *)
(*    race in ConsistencyMarkerSupport.updateAppendPosition (non-atomic    *)
(*    read-upperBound-write of the volatile marker: a concurrent update    *)
(*    can clobber a newer position with an older one). Expected:           *)
(*    SiblingNoFalseConflict is violated -- a single writer gets rejected  *)
(*    by its own history (MC_markerchain_regress.cfg).                     *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Writers,              \* the competing nodes, e.g. {w1, w2}
    MaxStore,             \* exploration bound on the store length (CONSTRAINT)
    ConditionalAppends,   \* TRUE = DCB fencing on (the shipped design)
    MarkerRegressEnabled  \* TRUE = enable the updateAppendPosition lost-update race

ASSUME MaxStoreAssumption == MaxStore \in Nat \ {0}

NoMarker == -1                 \* "no marker yet" (fresh writer, pre-spawn)
Steps == {"s1"}                \* one step suffices for the per-step invariants
Kinds == {"Started", "Retrying", "StepTerm", "WfTerminal"}

VARIABLES
    store,        \* Seq of [writer, kind, step, mkr]: the durable DCB store;
                  \* position = index; mkr = the marker the append was checked from
    marker,       \* [Writers -> {NoMarker} \union Nat]: per-writer ConsistencyMarker
    intent,       \* [Writers -> {"unstarted","step","terminal","done"}]: the
                  \* writer's replay-derived program position (what it appends next)
    interrupted,  \* [Writers -> BOOLEAN]: rejected => interrupted, appends nothing more
    active,       \* SUBSET Writers (history): writers that ever ATTEMPTED an append
    everRejected  \* BOOLEAN (history): some append was ever rejected

vars == <<store, marker, intent, interrupted, active, everRejected>>

Rec == [writer: Writers, kind: Kinds, step: Steps \union {"none"}, mkr: Nat]

TypeOK ==
    /\ store \in Seq(Rec)
    /\ marker \in [Writers -> {NoMarker} \union Nat]
    /\ intent \in [Writers -> {"unstarted", "step", "terminal", "done"}]
    /\ interrupted \in [Writers -> BOOLEAN]
    /\ active \subseteq Writers
    /\ everRejected \in BOOLEAN

Init ==
    /\ store = <<>>
    /\ marker = [w \in Writers |-> NoMarker]
    /\ intent = [w \in Writers |-> "unstarted"]
    /\ interrupted = [w \in Writers |-> FALSE]
    /\ active = {}
    /\ everRejected = FALSE

\* DCB condition (single instance, so every record matches the criteria):
\* commit iff nothing exists at a position > m.
CanCommit(m) == ~ConditionalAppends \/ m >= Len(store)
IsRejected(m) == ConditionalAppends /\ m < Len(store)

\* A successful append: record takes the next position, the writer's marker
\* advances to that position.
Commit(w, k, s, m) ==
    /\ store' = Append(store, [writer |-> w, kind |-> k, step |-> s, mkr |-> m])
    /\ marker' = [marker EXCEPT ![w] = Len(store) + 1]
    /\ UNCHANGED <<interrupted, everRejected>>

\* Fresh spawn: no marker, checked from ORIGIN (m = 0).
SpawnOk(w) ==
    /\ ~interrupted[w] /\ intent[w] = "unstarted" /\ marker[w] = NoMarker
    /\ CanCommit(0)
    /\ Commit(w, "Started", "none", 0)
    /\ intent' = [intent EXCEPT ![w] = "step"]
    /\ active' = active \union {w}

\* A repeatable non-terminal step record (RETRYING-like): the same writer can
\* append it any number of times; each repeat is checked from the marker its
\* previous append wrote at, so a lone writer is never rejected.
RetryOk(w) ==
    /\ ~interrupted[w] /\ intent[w] = "step"
    /\ CanCommit(marker[w])
    /\ Commit(w, "Retrying", "s1", marker[w])
    /\ intent' = intent
    /\ active' = active \union {w}

\* The step's terminal outcome record.
StepOk(w) ==
    /\ ~interrupted[w] /\ intent[w] = "step"
    /\ CanCommit(marker[w])
    /\ Commit(w, "StepTerm", "s1", marker[w])
    /\ intent' = [intent EXCEPT ![w] = "terminal"]
    /\ active' = active \union {w}

\* The workflow's own terminal event.
TermOk(w) ==
    /\ ~interrupted[w] /\ intent[w] = "terminal"
    /\ CanCommit(marker[w])
    /\ Commit(w, "WfTerminal", "none", marker[w])
    /\ intent' = [intent EXCEPT ![w] = "done"]
    /\ active' = active \union {w}

\* The writer's next append (whichever its intent dictates) is REJECTED:
\* another writer recorded events past its marker. The writer is interrupted
\* and publishes nothing further; the marker does NOT advance.
RejectedAppend(w) ==
    /\ ~interrupted[w]
    /\ \/ (intent[w] = "unstarted" /\ marker[w] = NoMarker /\ IsRejected(0))
       \/ (intent[w] \in {"step", "terminal"} /\ IsRejected(marker[w]))
    /\ interrupted' = [interrupted EXCEPT ![w] = TRUE]
    /\ everRejected' = TRUE
    /\ active' = active \union {w}
    /\ UNCHANGED <<store, marker, intent>>

\* Restore (segment claim): seed the marker from the current head of the store
\* and rebuild the program position by replay. Requires the instance to exist
\* (a claim sources an existing instance). The PREVIOUS owner is not told --
\* it keeps its old marker and intent (the stale writer the fencing must stop).
Restore(w) ==
    /\ ~interrupted[w]
    /\ \E i \in DOMAIN store : store[i].kind = "Started"
    /\ marker' = [marker EXCEPT ![w] = Len(store)]
    /\ intent' = [intent EXCEPT ![w] =
           IF \E i \in DOMAIN store : store[i].kind = "WfTerminal" THEN "done"
           ELSE IF \E i \in DOMAIN store : store[i].kind = "StepTerm" THEN "terminal"
           ELSE "step"]
    /\ UNCHANGED <<store, interrupted, active, everRejected>>

\* The updateAppendPosition lost-update race: a concurrent seed/update clobbers
\* the volatile marker with an OLDER position (read-upperBound-write is not
\* atomic). Only enabled in MC_markerchain_regress.cfg.
MarkerRegress(w) ==
    /\ MarkerRegressEnabled
    /\ ~interrupted[w]
    /\ marker[w] >= 1
    /\ \E v \in 0..(marker[w] - 1) : marker' = [marker EXCEPT ![w] = v]
    /\ UNCHANGED <<store, intent, interrupted, active, everRejected>>

Next == \E w \in Writers :
    \/ SpawnOk(w)
    \/ RetryOk(w)
    \/ StepOk(w)
    \/ TermOk(w)
    \/ RejectedAppend(w)
    \/ Restore(w)
    \/ MarkerRegress(w)

Spec == Init /\ [][Next]_vars

\* Exploration bound (cfg CONSTRAINT), not part of the protocol.
StoreBound == Len(store) <= MaxStore

-------------------------------------------------------------------------------
\* Invariants

\* At most one Started record for the instance, ever.
OneStartPerInstance ==
    Cardinality({i \in DOMAIN store : store[i].kind = "Started"}) <= 1

\* Per step, at most one terminal outcome record.
AtMostOnceRecording ==
    \A s \in Steps :
        Cardinality({i \in DOMAIN store :
                        store[i].kind = "StepTerm" /\ store[i].step = s}) <= 1

\* Ownership loss is not observable to the writer, so the real guarantee is the
\* condition semantics on the store itself: every committed record was appended
\* by a writer whose marker covered the ENTIRE prefix before it (mkr >= its
\* position - 1). A stale writer's append -- its marker predating another
\* writer's later event -- can therefore never commit; a writer only writes
\* after another writer iff it (re-)seeded past that writer's events.
NoWriteAfterOwnershipLoss ==
    \A i \in DOMAIN store : store[i].mkr >= i - 1

\* Serialized-chain guarantee: while only ONE writer has ever attempted an
\* append, no rejection occurs -- a writer is never fenced out by its own
\* history (repeats of the same writer always pass).
SiblingNoFalseConflict ==
    Cardinality(active) <= 1 => ~everRejected

\* Action property (PROPERTY, not INVARIANT): a rejection never advances the
\* rejected writer's marker.
RejectionNeverAdvancesMarker ==
    [][\A w \in Writers :
          (~interrupted[w] /\ interrupted'[w]) => marker'[w] = marker[w]]_vars

===============================================================================
