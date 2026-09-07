------------------------------ MODULE ClaimSeed ------------------------------
(***************************************************************************)
(* Design-level model of the position a segment claim seeds each restored  *)
(* workflow instance with, and of the only thing that position has to      *)
(* guarantee: a restored instance is never rejected by its OWN history.    *)
(*                                                                         *)
(* MarkerChain.tla models one instance, so it cannot express the choice    *)
(* this module is about. A claim restores several instances at once, and   *)
(* the seed can be taken in two ways:                                      *)
(*                                                                         *)
(*  - PerInstanceSeed = TRUE  -- each instance is sourced in a unit of     *)
(*    work of its own, so its seed is the head of the store at ITS OWN     *)
(*    read ("Seed each restored workflow from its own sourcing position"). *)
(*                                                                         *)
(*  - PerInstanceSeed = FALSE -- every instance of the claim shares one    *)
(*    transaction, whose append position is the LOWEST of its reads        *)
(*    (AF DefaultEventStoreTransaction.updateAppendPosition takes the      *)
(*    lower bound). An instance read late is then seeded before its own    *)
(*    last event.                                                          *)
(*                                                                         *)
(* The adversary is the previous owner: it does not observe the claim, so  *)
(* it keeps appending for the instances it still thinks it owns, including *)
(* in between two reads of the claim.                                      *)
(*                                                                         *)
(* Expected: NoSelfFence holds under PerInstanceSeed and is violated with  *)
(* the shared seed (MC_claimseed.cfg / MC_claimseed_shared.cfg) -- the     *)
(* refutable twin that makes the green run mean something.                 *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Instances,       \* the instances one claim restores, e.g. {i1, i2}
    MaxStore,        \* exploration bound on the store length (CONSTRAINT)
    PerInstanceSeed  \* TRUE = each instance seeds from its own read

ASSUME MaxStoreAssumption == MaxStore \in Nat \ {0}

NoSeed == -1

VARIABLES
    store,        \* Seq of [inst, writer]: the shared durable store; position = index
    seed,         \* [Instances -> {NoSeed} \union Nat]: the restored execution's marker
    low,          \* Nat: the claim transaction's running lower bound over its reads
    readDone,     \* [Instances -> BOOLEAN]: whether the claim already read the instance
    foreignAfter, \* [Instances -> BOOLEAN] (history): a previous owner wrote after that read
    selfFenced    \* [Instances -> BOOLEAN] (history): rejected with no foreign write after the read

vars == <<store, seed, low, readDone, foreignAfter, selfFenced>>

Rec == [inst: Instances, writer: {"old", "new"}]

TypeOK ==
    /\ store \in Seq(Rec)
    /\ seed \in [Instances -> {NoSeed} \union (0..MaxStore)]
    /\ low \in 0..MaxStore
    /\ readDone \in [Instances -> BOOLEAN]
    /\ foreignAfter \in [Instances -> BOOLEAN]
    /\ selfFenced \in [Instances -> BOOLEAN]

Init ==
    /\ store = <<>>
    /\ seed = [i \in Instances |-> NoSeed]
    /\ low = MaxStore            \* nothing read yet: the lower bound is still open
    /\ readDone = [i \in Instances |-> FALSE]
    /\ foreignAfter = [i \in Instances |-> FALSE]
    /\ selfFenced = [i \in Instances |-> FALSE]

Min(a, b) == IF a < b THEN a ELSE b

\* The previous owner appends for an instance. It never learns it lost the claim, so it can do
\* this before, between or after the claim's reads.
ForeignAppend(i) ==
    /\ Len(store) < MaxStore
    /\ store' = Append(store, [inst |-> i, writer |-> "old"])
    /\ foreignAfter' = [foreignAfter EXCEPT ![i] = readDone[i] \/ @]
    /\ UNCHANGED <<seed, low, readDone, selfFenced>>

\* The claim reads one instance and restores it at the position that read leaves it at.
Seed(i) ==
    /\ ~readDone[i]
    /\ LET readAt == Len(store)
           bound  == Min(readAt, low)
       IN /\ low' = bound
          /\ seed' = [seed EXCEPT ![i] = IF PerInstanceSeed THEN readAt ELSE bound]
    /\ readDone' = [readDone EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<store, foreignAfter, selfFenced>>

\* The restored execution's next append, conditioned on the instance's own criteria: rejected iff a
\* record for THIS instance sits past its marker. A rejection with no foreign write after the read is
\* the instance fencing itself out on its own history.
RestoredAppend(i) ==
    /\ readDone[i]
    /\ IF \E k \in DOMAIN store : store[k].inst = i /\ k > seed[i]
       THEN /\ selfFenced' = [selfFenced EXCEPT ![i] = ~foreignAfter[i] \/ @]
            /\ UNCHANGED <<store, seed, low, readDone, foreignAfter>>
       ELSE /\ Len(store) < MaxStore
            /\ store' = Append(store, [inst |-> i, writer |-> "new"])
            /\ seed' = [seed EXCEPT ![i] = Len(store) + 1]
            /\ UNCHANGED <<low, readDone, foreignAfter, selfFenced>>

Next == \E i \in Instances :
    \/ ForeignAppend(i)
    \/ Seed(i)
    \/ RestoredAppend(i)

Spec == Init /\ [][Next]_vars

StoreBound == Len(store) <= MaxStore

-------------------------------------------------------------------------------
\* Invariants

\* A restored instance is never rejected by its own history: only a write that lands after its own
\* sourcing read may stop it, and such a write is a foreign writer, which is what should stop it.
NoSelfFence == \A i \in Instances : ~selfFenced[i]

===============================================================================
