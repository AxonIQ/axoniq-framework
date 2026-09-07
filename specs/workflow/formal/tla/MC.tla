---------------------------------- MODULE MC ----------------------------------
(***************************************************************************)
(* Model-checking harness for WorkflowLeaseRecovery.  Pins the tiny scope  *)
(* (2 procs, 3 ordered steps, bounded log & crashes).  The two BOOLEAN     *)
(* flags DURABLE_LEASE and APPEND_CONDITION are supplied per .cfg so each   *)
(* invariant gets a clean run:                                             *)
(*   MC_safe.cfg   - DURABLE_LEASE=TRUE,  APPEND_CONDITION=TRUE             *)
(*   MC_effect.cfg - DURABLE_LEASE=TRUE,  APPEND_CONDITION=FALSE  (F-0)     *)
(*   MC_owner.cfg  - DURABLE_LEASE=FALSE, APPEND_CONDITION=TRUE   (F-1)     *)
(*   MC_live.cfg   - liveness (FairSpec)                                    *)
(*   MC_effect_fixed.cfg / MC_owner_fixed.cfg - fix flags ON (No error)     *)
(***************************************************************************)
EXTENDS WorkflowLeaseRecovery

\* Symmetry over the two interchangeable processes shrinks the safety state
\* space.  NOT used for the liveness run (MC_live.cfg) -- TLC warns that
\* symmetry can cause it to miss liveness-property violations.
ProcSymmetry == Permutations(Procs)

===============================================================================
