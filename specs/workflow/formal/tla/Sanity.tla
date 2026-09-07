-------------------------------- MODULE Sanity --------------------------------
\* Step A: a trivial 1-variable counter that proves the TLC + .cfg wiring works
\* before we run the real WorkflowLeaseRecovery model. This documents the smoke
\* test; keep it in the repo. (Phase 2, POC tla_dst.)
EXTENDS Naturals

VARIABLE x

Init == x = 0

Next == x' = (x + 1) % 4   \* bounded so the state space is finite

Spec == Init /\ [][Next]_x

\* Trivial safety invariant: the counter stays inside its declared bound.
TypeOK == x \in 0..3

===============================================================================
