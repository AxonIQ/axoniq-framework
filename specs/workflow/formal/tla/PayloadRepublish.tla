--------------------------- MODULE PayloadRepublish ---------------------------
(***************************************************************************)
(* Design-level model of the modifyPayload duplicate terminal record       *)
(* across a crash + re-run (finding F-7, corroborating the DST             *)
(* F7DuplicatePayloadRecordTest / DuplicateTerminalPayloadRecordScenario   *)
(* pin).                                                                   *)
(*                                                                         *)
(* PayloadDelegate.modifyPayload is the ONLY state-publishing primitive    *)
(* that gates only the drift guard on !containsStep(stepName) — NOT the    *)
(* publish — appends its publish task UNCONDITIONALLY, and publishes the   *)
(* <step>:COMPLETED event DIRECTLY via the event sink, bypassing the       *)
(* guarded sendStepEvent path ("step already terminal -> refuse to         *)
(* publish"). When the step's COMPLETED has durably committed and the      *)
(* process crashes BEFORE the workflow's own terminal commit (the instance *)
(* stays non-terminal), the post-crash recovery re-runs the body,          *)
(* re-reaches the modifyPayload call with the step already present, and    *)
(* re-publishes a SECOND identical <step>:COMPLETED — two terminal records *)
(* for one (workflowId, stepName), an INV-2 AtMostOnceRecording violation  *)
(* (distinct from F-0, which duplicates an EFFECT while the record stays   *)
(* <= 1; F-7 duplicates the RECORD itself).                                *)
(*                                                                         *)
(* The model: one instance with two steps run live to their committed      *)
(* COMPLETED — "payload" (a modifyPayload step) and "exec" (a GATED        *)
(* execute-style contrast step). A crash MAY land between the steps'       *)
(* COMPLETED commits and the workflow terminal; the recovered body re-runs *)
(* both steps. The exec step gates its publish on !containsStep and routes *)
(* through the guarded path, so it never re-publishes (Execute/WaitFor/    *)
(* Version all behave this way). The payload step re-publishes             *)
(* unconditionally unless the fix toggle CONTAINS_STEP_GATE puts the same  *)
(* replay-skip gate (and/or the guarded sendStepEvent route) in front of   *)
(* its publish.                                                            *)
(*                                                                         *)
(* AtMostOnceRecording: at most one terminal record per step in the        *)
(* durable log.                                                            *)
(*   CONTAINS_STEP_GATE = FALSE -> VIOLATED (MC_payload.cfg) — the re-run  *)
(*                                 commits a second payload-step COMPLETED *)
(*                                 (the exec contrast step stays at 1)     *)
(*   CONTAINS_STEP_GATE = TRUE  -> holds    (MC_payload_fixed.cfg)         *)
(***************************************************************************)
EXTENDS Naturals

CONSTANT CONTAINS_STEP_GATE  \* TRUE = modifyPayload's publish is gated on !containsStep (the modeled fix).

Steps == {"payload", "exec"}

VARIABLES
    records,  \* [Steps -> Nat]: terminal <step>:COMPLETED records in the durable log
    phase     \* "live" -> "committed" -> ("finished" | "crashed" -> "rerun" -> "finished")

vars == <<records, phase>>

Init ==
    /\ records = [s \in Steps |-> 0]
    /\ phase = "live"

\* The live run: each step publishes its <step>:COMPLETED and the record
\* durably commits (the payload step directly via the event sink, the exec
\* step via the guarded sendStepEvent path — same durable outcome live).
LiveRun ==
    /\ phase = "live"
    /\ records' = [s \in Steps |-> 1]
    /\ phase' = "committed"

\* The workflow's own terminal status commits — the instance is terminal,
\* recovery will not re-drive it. (No duplicate on this branch.)
FinishWorkflow ==
    /\ phase = "committed"
    /\ phase' = "finished"
    /\ UNCHANGED records

\* The crash window: AFTER the steps' COMPLETED commits, BEFORE the workflow's
\* terminal commit — the instance stays non-terminal, so recovery re-runs it.
Crash ==
    /\ phase = "committed"
    /\ phase' = "crashed"
    /\ UNCHANGED records

\* The recovered body re-runs and re-reaches both steps, each already present
\* in the rebuilt state. The exec step gates on !containsStep AND routes
\* through the guarded sendStepEvent, so it SKIPS the re-publish. The payload
\* step appends its publish task unconditionally and publishes directly —
\* unless the modeled fix gates it the same way.
Rerun ==
    /\ phase = "crashed"
    /\ records' = [records EXCEPT !["payload"] =
                       IF CONTAINS_STEP_GATE THEN @ ELSE @ + 1]
    /\ phase' = "rerun"

\* The re-run then completes the workflow (its terminal commits).
RerunFinish ==
    /\ phase = "rerun"
    /\ phase' = "finished"
    /\ UNCHANGED records

Next == LiveRun \/ FinishWorkflow \/ Crash \/ Rerun \/ RerunFinish

Spec == Init /\ [][Next]_vars

\* THE property (INV-2 / F-7): at most one terminal record per step in the
\* durable log.
AtMostOnceRecording ==
    \A s \in Steps : records[s] <= 1

=============================================================================
