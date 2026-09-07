---------------------------- MODULE CombinatorReplay ----------------------------
(***************************************************************************)
(* Design-level model of the combinator categorization across a            *)
(* crash/replay (finding F-22, corroborating the DST                       *)
(* CombinatorReplayTest.allMatch_… pin).                                   *)
(*                                                                         *)
(* The engine's combinators (anyMatch/allMatch/noneMatch) compute their    *)
(* matched()/unmatched() categorization LAZILY, PER BODY RUN, from the     *)
(* branches' CURRENT step states (CombinatorSupport.computeCategories) —   *)
(* nothing about the categorization is ever recorded durably. A branch     *)
(* still in flight when the live body observes the categorization can      *)
(* complete BEFORE a crash; the recovered re-run then recomputes the       *)
(* categorization over the now-terminal cached state and observes a        *)
(* DIFFERENT membership. The combinator DECISION (winner/violator) is      *)
(* derived from the durable first-completed order and stays stable; only  *)
(* the category membership diverges.                                       *)
(*                                                                         *)
(* The model: three branches a/b/c. a completes matching, b completes      *)
(* non-matching (the allMatch short-circuit trigger), c is in flight at    *)
(* the live observation and MAY complete before the crash. The fix toggle  *)
(* SNAPSHOT_CATEGORIES models recording the live categorization durably at *)
(* decision time, with the replay reading the record instead of            *)
(* recomputing.                                                            *)
(*                                                                         *)
(* CategoriesReplayStable: once the replay has observed its                *)
(* categorization, it equals what the live run observed.                   *)
(*   SNAPSHOT_CATEGORIES = FALSE -> VIOLATED (MC_combinator.cfg)           *)
(*   SNAPSHOT_CATEGORIES = TRUE  -> holds    (MC_combinator_fixed.cfg)     *)
(***************************************************************************)
EXTENDS Naturals

CONSTANT SNAPSHOT_CATEGORIES   \* TRUE = record the live categorization durably; replay reads the record.

Branches == {"a", "b", "c"}

\* Branch step statuses: "running" (in flight), "match" (terminal, satisfies the
\* predicate), "nomatch" (terminal, fails the predicate). Terminal statuses are
\* durable (event-sourced) and survive the crash; "running" does not need to —
\* the model lets c either stay running or complete before the crash.
VARIABLES
    status,      \* [Branches -> {"running", "match", "nomatch"}]
    liveCats,    \* the matched-set the LIVE body observed, or "none" before it observed
    recordedCats,\* the durably recorded matched-set (the modeled fix), or "none"
    replayCats,  \* the matched-set the REPLAYED body observed, or "none"
    phase        \* "live" -> "observed" -> "crashed" -> "replayed"

vars == <<status, liveCats, recordedCats, replayCats, phase>>

\* The categorization the engine computes from CURRENT branch states: the set of
\* branches that are terminal AND satisfy the predicate (everything else —
\* including still-running branches — is unmatched, the C-5 shape).
Categorize(st) == {br \in Branches : st[br] = "match"}

Init ==
    /\ status = [br \in Branches |-> IF br = "a" THEN "match"
                                     ELSE IF br = "b" THEN "nomatch"
                                     ELSE "running"]
    /\ liveCats = "none"
    /\ recordedCats = "none"
    /\ replayCats = "none"
    /\ phase = "live"

\* The live body observes the combinator: the allMatch short-circuited on b (a
\* terminal non-match exists), and the body reads matched()/unmatched() NOW —
\* with c still running. Under the fix the observation is also recorded durably.
ObserveLive ==
    /\ phase = "live"
    /\ liveCats' = Categorize(status)
    /\ recordedCats' = IF SNAPSHOT_CATEGORIES THEN Categorize(status) ELSE "none"
    /\ phase' = "observed"
    /\ UNCHANGED <<status, replayCats>>

\* The in-flight branch completes (either way) AFTER the live observation,
\* BEFORE the crash — its terminal status is durably committed.
LateComplete ==
    /\ phase = "observed"
    /\ status["c"] = "running"
    /\ \E outcome \in {"match", "nomatch"} : status' = [status EXCEPT !["c"] = outcome]
    /\ UNCHANGED <<liveCats, recordedCats, replayCats, phase>>

\* The crash: volatile state is lost; the durable statuses (terminal records)
\* and the durable categorization record (if the fix is on) survive.
Crash ==
    /\ phase = "observed"
    /\ phase' = "crashed"
    /\ UNCHANGED <<status, liveCats, recordedCats, replayCats>>

\* The recovered body re-runs and observes the combinator again: without the
\* fix it RECOMPUTES from the (now possibly different) durable statuses; with
\* the fix it reads the durable record.
Replay ==
    /\ phase = "crashed"
    /\ replayCats' = IF SNAPSHOT_CATEGORIES THEN recordedCats ELSE Categorize(status)
    /\ phase' = "replayed"
    /\ UNCHANGED <<status, liveCats, recordedCats>>

Next == ObserveLive \/ LateComplete \/ Crash \/ Replay

Spec == Init /\ [][Next]_vars

\* THE property (F-22): what the replayed body observes from the combinator
\* equals what the live body observed.
CategoriesReplayStable ==
    (phase = "replayed") => (replayCats = liveCats)

=============================================================================
