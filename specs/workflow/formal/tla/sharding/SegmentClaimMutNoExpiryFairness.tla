-------------------------------- MODULE SegmentClaimMutNoExpiryFairness --------------------------------
(***************************************************************************)
(* Segment claim protocol of the workflow engine's sharding feature.       *)
(*                                                                         *)
(* Grounded in (read at commit 9584bb65 / axon-messaging 5.3.0):           *)
(*  - WorkflowEventProcessingRegistrationEnhancer                          *)
(*      ensureSegmentsInitialized / earliestSegmentToken /                 *)
(*      fetchTokenAndReleaseClaim: startup walks EVERY segment and         *)
(*      tokenStore.fetchToken(..) CLAIMS it, then releaseClaim(..).        *)
(*      A fetchToken on a foreign, unexpired claim throws                  *)
(*      UnableToClaimTokenException, which fails the start handler and     *)
(*      therefore the whole application boot.                              *)
(*  - Coordinator.abortWorkPackage (~1517-1522):                           *)
(*      work.onSegmentReleased(ctx)                                        *)
(*        -> tokenStore.releaseClaim(name, segmentId, ctx)                 *)
(*        -> segmentChangeListener.onSegmentReleased(segment)              *)
(*      i.e. the claim is dropped BEFORE WorkflowEngine.releaseSegment     *)
(*      interrupts/removes the running workflow executions.                *)
(*  - Coordinator.createWorkPackage: claim -> onSegmentClaimed ->          *)
(*      WorkflowEngine.claimSegment, which rehydrates and STARTS the       *)
(*      instances the segment owns (SegmentedWorkflowRouting.shouldHandle).*)
(*  - JdbcTokenEntry.mayClaim: owner == null || owner.equals(self)         *)
(*      || timestamp + claimTimeout < now.                                 *)
(*  - JdbcTokenStoreConfiguration: nodeId defaults to                      *)
(*      ManagementFactory.getRuntimeMXBean().getName() = "pid@host",       *)
(*      so a restarted JVM is a DIFFERENT owner to its own leftover rows.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,                     \* one JVM / processor instance each
    Segments,                  \* segments of the "Workflow" processor
    MaxCrashes,                \* bound on total crashes (keeps TLC finite)
    StartupClaimsAllSegments,  \* TRUE  = as implemented (fetchToken claims)
    ReleaseClaimBeforeDrain,   \* TRUE  = as implemented (Coordinator:1517-1522)
    StableNodeId,              \* FALSE = as implemented (nodeId is pid@host)
    LiveClaimsExpire           \* FALSE = default reading (holder renews in time)

NoOwner == "none"
Ghost   == "ghost"                 \* row left by a dead JVM under a dead pid
Owners  == Nodes \cup {NoOwner, Ghost}
Phases  == {"down", "starting", "running"}

VARIABLES
    owner,    \* [Segments -> Owners]        token-store owner column
    phase,    \* [Nodes -> Phases]           node lifecycle
    scanned,  \* [Nodes -> SUBSET Segments]  segments the startup scan finished
    held,     \* [Nodes -> SUBSET Segments]  claims this node believes it holds
    exec,     \* [Nodes -> SUBSET Segments]  segments whose instances it runs
    crashes

vars == <<owner, phase, scanned, held, exec, crashes>>

Live(n)      == phase[n] # "down"
MayClaim(n, s) == owner[s] = NoOwner \/ owner[s] = n   \* JdbcTokenEntry.mayClaim
Abandoned(s) == /\ owner[s] # NoOwner
                /\ ~(\E n \in Nodes : owner[s] = n /\ s \in held[n])

TypeOK ==
    /\ owner   \in [Segments -> Owners]
    /\ phase   \in [Nodes -> Phases]
    /\ scanned \in [Nodes -> SUBSET Segments]
    /\ held    \in [Nodes -> SUBSET Segments]
    /\ exec    \in [Nodes -> SUBSET Segments]
    /\ crashes \in 0..MaxCrashes

Init ==
    /\ owner   = [s \in Segments |-> NoOwner]
    /\ phase   = [n \in Nodes |-> "down"]
    /\ scanned = [n \in Nodes |-> {}]
    /\ held    = [n \in Nodes |-> {}]
    /\ exec    = [n \in Nodes |-> {}]
    /\ crashes = 0

(***************************************************************************)
(* Startup: PRE_PROCESSOR_START_PHASE hook -> earliestSegmentToken.        *)
(* The futures for all segments are created eagerly in the for loop, so    *)
(* claim and release of different segments interleave arbitrarily.         *)
(***************************************************************************)
StartBegin(n) ==
    /\ phase[n] = "down"
    /\ phase'   = [phase   EXCEPT ![n] = "starting"]
    /\ scanned' = [scanned EXCEPT ![n] = {}]
    /\ held'    = [held    EXCEPT ![n] = {}]
    /\ exec'    = [exec    EXCEPT ![n] = {}]
    /\ UNCHANGED <<owner, crashes>>

ScanClaim(n, s) ==                      \* tokenStore.fetchToken -> claims
    /\ StartupClaimsAllSegments
    /\ phase[n] = "starting"
    /\ s \notin scanned[n] /\ s \notin held[n]
    /\ MayClaim(n, s)
    /\ owner' = [owner EXCEPT ![s] = n]
    /\ held'  = [held  EXCEPT ![n] = @ \cup {s}]
    /\ UNCHANGED <<phase, scanned, exec, crashes>>

ScanRelease(n, s) ==                    \* tokenStore.releaseClaim
    /\ StartupClaimsAllSegments
    /\ phase[n] = "starting"
    /\ s \in held[n]
    /\ owner'   = [owner   EXCEPT ![s] = NoOwner]
    /\ held'    = [held    EXCEPT ![n] = @ \ {s}]
    /\ scanned' = [scanned EXCEPT ![n] = @ \cup {s}]
    /\ UNCHANGED <<phase, exec, crashes>>

ScanNoClaim(n, s) ==                    \* candidate fix: read without claiming
    /\ ~StartupClaimsAllSegments
    /\ phase[n] = "starting"
    /\ s \notin scanned[n]
    /\ scanned' = [scanned EXCEPT ![n] = @ \cup {s}]
    /\ UNCHANGED <<owner, phase, held, exec, crashes>>

StartFail(n) ==                         \* UnableToClaimTokenException -> boot fails
    /\ StartupClaimsAllSegments
    /\ phase[n] = "starting"
    /\ \E s \in Segments : /\ s \notin scanned[n] /\ s \notin held[n]
                           /\ ~MayClaim(n, s)
    /\ owner' = [s \in Segments |-> IF s \in held[n] THEN NoOwner ELSE owner[s]]
    /\ phase' = [phase EXCEPT ![n] = "down"]
    /\ held'  = [held  EXCEPT ![n] = {}]
    /\ exec'  = [exec  EXCEPT ![n] = {}]
    /\ UNCHANGED <<scanned, crashes>>

FinishStart(n) ==
    /\ phase[n] = "starting"
    /\ scanned[n] = Segments
    /\ phase' = [phase EXCEPT ![n] = "running"]
    /\ UNCHANGED <<owner, scanned, held, exec, crashes>>

(***************************************************************************)
(* Steady state: coordinator claims, engine rehydrates, coordinator drops. *)
(***************************************************************************)
CoordinatorClaim(n, s) ==               \* fetchAvailableSegments + claimToken
    /\ phase[n] = "running"
    /\ s \notin held[n]
    /\ owner[s] = NoOwner
    /\ owner' = [owner EXCEPT ![s] = n]
    /\ held'  = [held  EXCEPT ![n] = @ \cup {s}]
    /\ UNCHANGED <<phase, scanned, exec, crashes>>

EngineClaim(n, s) ==                    \* onSegmentClaimed -> claimSegment
    /\ phase[n] = "running"
    /\ s \in held[n] /\ s \notin exec[n]
    /\ exec' = [exec EXCEPT ![n] = @ \cup {s}]
    /\ UNCHANGED <<owner, phase, scanned, held, crashes>>

ReleaseClaimFirst(n, s) ==              \* as implemented: releaseClaim then listener
    /\ ReleaseClaimBeforeDrain
    /\ phase[n] = "running"
    /\ s \in held[n]
    /\ owner' = [owner EXCEPT ![s] = IF owner[s] = n THEN NoOwner ELSE owner[s]]
    /\ held'  = [held  EXCEPT ![n] = @ \ {s}]
    /\ UNCHANGED <<phase, scanned, exec, crashes>>

DrainAfterRelease(n, s) ==              \* releaseSegment: interrupt + removeAll
    /\ ReleaseClaimBeforeDrain
    /\ phase[n] = "running"
    /\ s \in exec[n] /\ s \notin held[n]
    /\ exec' = [exec EXCEPT ![n] = @ \ {s}]
    /\ UNCHANGED <<owner, phase, scanned, held, crashes>>

ReleaseDrained(n, s) ==                 \* candidate fix: drain, then release claim
    /\ ~ReleaseClaimBeforeDrain
    /\ phase[n] = "running"
    /\ s \in held[n]
    /\ owner' = [owner EXCEPT ![s] = IF owner[s] = n THEN NoOwner ELSE owner[s]]
    /\ held'  = [held  EXCEPT ![n] = @ \ {s}]
    /\ exec'  = [exec  EXCEPT ![n] = @ \ {s}]
    /\ UNCHANGED <<phase, scanned, crashes>>

(***************************************************************************)
(* Faults and time.                                                        *)
(***************************************************************************)
Crash(n) ==
    /\ crashes < MaxCrashes
    /\ Live(n)
    /\ owner'   = [s \in Segments |->
                     IF owner[s] = n /\ ~StableNodeId THEN Ghost ELSE owner[s]]
    /\ phase'   = [phase EXCEPT ![n] = "down"]
    /\ held'    = [held  EXCEPT ![n] = {}]
    /\ exec'    = [exec  EXCEPT ![n] = {}]
    /\ crashes' = crashes + 1
    /\ UNCHANGED scanned

ExpireClaim(s) ==                       \* timestamp + claimTimeout < now
    /\ Abandoned(s)
    /\ owner' = [owner EXCEPT ![s] = NoOwner]
    /\ UNCHANGED <<phase, scanned, held, exec, crashes>>

StealLiveClaim(n, s) ==   \* alternative reading: holder missed its extendClaim
    /\ LiveClaimsExpire
    /\ phase[n] = "running"
    /\ \E m \in Nodes : m # n /\ owner[s] = m /\ s \in held[m] /\ Live(m)
    /\ owner' = [owner EXCEPT ![s] = n]
    /\ held'  = [held  EXCEPT ![n] = @ \cup {s}]
    /\ UNCHANGED <<phase, scanned, exec, crashes>>

Next ==
    \/ \E n \in Nodes : StartBegin(n) \/ StartFail(n) \/ FinishStart(n) \/ Crash(n)
    \/ \E n \in Nodes, s \in Segments :
          \/ ScanClaim(n, s) \/ ScanRelease(n, s) \/ ScanNoClaim(n, s)
          \/ CoordinatorClaim(n, s) \/ EngineClaim(n, s)
          \/ ReleaseClaimFirst(n, s) \/ DrainAfterRelease(n, s) \/ ReleaseDrained(n, s)
          \/ StealLiveClaim(n, s)
    \/ \E s \in Segments : ExpireClaim(s)

(***************************************************************************)
(* Fairness. Progress steps are fair; faults (Crash, StealLiveClaim) and   *)
(* voluntary rebalancing (the two release actions) are NOT, so nothing is  *)
(* forced to go wrong. ExpireClaim is fair: wall-clock time does pass.     *)
(* CoordinatorClaim and EngineClaim are STRONGLY fair: a segment that      *)
(* becomes claimable, or a claim that is held, only infinitely often (the  *)
(* coordinator polls, a segment can flap) must still eventually be taken   *)
(* up. In the code EngineClaim is not even a separate step: Coordinator.   *)
(* createWorkPackage invokes onSegmentClaimed synchronously after the      *)
(* claim, so anything weaker would be an artefact of splitting them.       *)
(***************************************************************************)
Fairness ==
    /\ \A n \in Nodes :
         /\ WF_vars(StartBegin(n)) /\ WF_vars(StartFail(n)) /\ WF_vars(FinishStart(n))
    /\ \A n \in Nodes, s \in Segments :
         /\ WF_vars(ScanClaim(n, s)) /\ WF_vars(ScanRelease(n, s))
         /\ WF_vars(ScanNoClaim(n, s))
         /\ SF_vars(CoordinatorClaim(n, s)) /\ SF_vars(EngineClaim(n, s))
         /\ WF_vars(DrainAfterRelease(n, s))
    /\ TRUE  \* MUTATION: WF_vars(ExpireClaim(s)) removed

Spec == Init /\ [][Next]_vars /\ Fairness

(* State constraint used by the *_seq configurations: nodes boot one at a  *)
(* time. Excludes the concurrent-boot interleavings so the remaining       *)
(* counter-example is the staggered "join a live cluster" case that was    *)
(* reproduced on real infrastructure.                                      *)
SequentialStarts == Cardinality({n \in Nodes : phase[n] = "starting"}) <= 1

(***************************************************************************)
(* Safety                                                                  *)
(***************************************************************************)
AtMostOneOwner ==
    \A n, m \in Nodes : (n # m /\ Live(n) /\ Live(m)) => held[n] \cap held[m] = {}

ClaimAgreement ==       \* a live node's belief matches the token-store row
    \A n \in Nodes : Live(n) => \A s \in held[n] : owner[s] = n

NoWorkAfterRelease ==   \* no instances executed for a segment no longer held
    \A n \in Nodes : exec[n] \subseteq held[n]

NoDuplicateExecution == \* the harm NoWorkAfterRelease guards against
    \A n, m \in Nodes : n # m => exec[n] \cap exec[m] = {}

BootNeverFails ==       \* no boot attempt ever throws UnableToClaimTokenException
    \A n \in Nodes : ~ENABLED StartFail(n)

(***************************************************************************)
(* Liveness                                                                *)
(***************************************************************************)
SomeoneRunning == \E n \in Nodes : phase[n] = "running"

NodeCanJoin ==
    \A n \in Nodes : (phase[n] = "starting") ~> (phase[n] = "running")

NoOrphanSegment ==
    \A s \in Segments :
        SomeoneRunning ~> (\E n \in Nodes : phase[n] = "running" /\ s \in held[n])

EveryInstanceEventuallyRuns ==
    \A s \in Segments : SomeoneRunning ~> (\E n \in Nodes : s \in exec[n])

=============================================================================
