# axon-flow-spec — Protocol Invariants (Phase 1)

Single source of truth for the task-leasing / crash-recovery properties. Each invariant has a
stable **`MachineName`** reused verbatim later as a TLA+ operator name (Phase 2 `.tla` spec) and as
a Java identifier in the DST simulator's assertions (Phase 4). All engine mappings cite
`file:LINE` against `formal/ARCHITECTURE.md` (the Phase-0 recon, branch `poc/tla_dst`).

**Vocabulary used below** (all engine-real, from ARCHITECTURE.md §§2–8):
- *Instance* = one `SimpleWorkflowExecution`, keyed by `workflowId`, holding a `Map<String,
  WorkflowStep>` of recorded steps (`EventSourcedWorkflowState.java:62`).
- *Step record* = a durable event for a `(stepName, status)` transition; statuses
  `STARTED/COMPLETED/FAILED/TIMED_OUT/CANCELLED/RETRYING` (`EventSourcedWorkflowState.java:200-248`).
  **Step names are the durable dedup keys** (§7).
- *Effect* = the user side-effect inside an `execute` action, `action.apply(...)`
  (`ExecuteDelegate.java:148`). Distinct from *recording* the COMPLETED event.
- *Owner / processing claim* = the process whose pooled-streaming event processor holds segment 0
  (one segment over all events, §2). There is no per-instance lease.
- *Safe point* = the durable engine recovery anchor = lower bound of active executions' restart
  tokens, stored in `SafePointStore` (ADR-004; `WorkflowEngine.java:317-331`).
- *Committed* = an event durably appended to the `EventStore` via `EventSink.publish`
  (`AbstractStepExecutor.java:222`).
- *Crash* = process loss; on restart the engine resets to the safe point and replays
  (`WorkflowEngine.java:152-180`, §5).

A deliberate distinction runs through this file (originally finding **F-0**): at-most-once **recording**
and at-most-once **execution of effects** are **separate** properties. INV-2 and INV-6 split them on
purpose so TLA+ and DST can probe each independently. The engine now guarantees **both** — F-0 is
**fixed** (an in-flight attempt is not re-run on resume; see INV-6). The split still matters: a crash
duplicated the *effect* while the *record* stayed ≤1, which is exactly why the two are tracked apart.

---

### INV-1: At-most-one owner of an instance's processing
- **Name:** `AtMostOneOwner`
- **Kind:** Safety
- **Plain English:** At any instant, at most one process is actively driving a given workflow
  instance's body and appending its step events. Two processes must never concurrently advance the
  same instance ("split-brain").
- **Formal-ish:** `∀ w ∈ Instances : Cardinality({ p ∈ Processes : owns(p, w) }) ≤ 1`, where
  `owns(p, w)` ⟺ `p`'s workflow event processor holds the claim on segment 0 **and** `p` routes
  events for `w` (per-`workflowId` routing inside the owning process,
  `WorkflowEngine.java:104-109`). The protocol-level property the TLA+ model asserts: claim
  acquisition on the segment is mutually exclusive across processes, so an event for `w` is
  delivered to the body of exactly one `SimpleWorkflowExecution` at a time.
- **Engine mapping:** Ownership is at the *whole-processor* granularity, not per instance (§2,
  `WorkflowEventProcessingRegistrationEnhancer.java:102-105,157`); a single segment covers all
  events (`AllEventEventHandlingComponent.java:58-73`, `.initialSegmentCount(1)`:72,
  `.batchSize(1)`:73). Claim/hold/expiry is Axon PSEP machinery over a `TokenStore`. **Threat:** that
  processor `TokenStore` is **hardcoded in-memory and non-durable** — `new InMemoryTokenStore()`
  (`AllEventEventHandlingComponent.java:70`); it is NOT injectable and Spring autoconfig never
  overrides `TokenStore[Workflow]` (§4 table, §13.1). So across nodes each process has its own
  in-memory claim — there is no cross-node hand-off and nothing forbids two nodes both owning
  segment 0. Within a single process, the per-instance single-threaded task queue
  (`SimpleWorkflowExecution.java:92,346-353`) makes the property hold trivially.
- **Currently holds?** Partial — **F-1**. Holds within one process (single-segment, single-consumer
  queue). Does **not** hold across nodes by construction of this component: the in-memory processor
  token gives no durable single-writer lease (§2 "Net:"; UNCLEAR §13.1 whether production is
  single-node by design). State as the protocol *should* guarantee; let the model expose the gap when
  the token store is non-durable / two claimers are admitted.
- **Checked by:** both — TLA+ invariant (model the segment claim as a lock; check no two processes
  hold it while the token store is non-durable / a crash loses the claim) + DST post-step assertion
  (with the in-memory token, drive two engine instances over a shared event store and assert no
  instance's step is appended by two owners).

---

### INV-2: No step recorded twice (at-most-once recording)
- **Name:** `AtMostOnceRecording`
- **Kind:** Safety
- **Plain English:** Within one instance's history, a given step name reaches a terminal outcome at
  most once — no step is recorded as COMPLETED (or FAILED/CANCELLED/TIMED_OUT) twice. **Scope:** this is about *step*
  terminals only. A duplicate *workflow-status* terminal record (e.g. a second `<workflow>:CANCELLED`) is **out of INV-2's
  scope** and is **not** caught here — the assertion keys on the step name and skips events carrying no step status (see
  "Currently holds?"); a duplicate workflow-status terminal is the F-13 gap, observed only by INV-7 (`TerminalIsFinal`).
- **Formal-ish:** `∀ w ∈ Instances, ∀ s ∈ StepNames :
  Cardinality({ e ∈ history(w) : e.step = s ∧ e.status ∈ TerminalStepStatuses }) ≤ 1`. Equivalently:
  once `state[w].steps[s]` is terminal, every later transition for `(w, s)` is a no-op.
- **Engine mapping:** Enforced on two sides. *Apply side:* `evolve` ignores any step transition once
  the step is already terminal (`EventSourcedWorkflowState.java:195-199`); migration steps use
  `versions.putIfAbsent` first-writer-wins (`:293`). *Publish side:* `sendStepEvent` refuses to
  publish when the step is already terminal (`AbstractStepExecutor.java:204-212`) or the workflow is
  terminal (`:197-203`); `appendTask` callbacks re-check `!status().isTerminal()`
  (`ExecuteDelegate.java:194`, `WaitForDelegate.java:115,131,150`). On replay a present step yields a
  cached result and emits nothing (`ExecuteDelegate.java:117`, §7). Step-name strings are the durable
  keys that make this hold (§3.4 of the skill).
- **Currently holds?** Yes for `execute`/`waitFor`/`migrateVersion`/`modifyPayload` — the engine's designed at-most-once
  *recording* guarantee (§7 "At-most-once recording"). Independent of F-0 (which is about effects, not records).
  Caveat: holds *given* INV-1 — if two owners append concurrently with no optimistic append-condition (FIXMEs at
  `ExecuteDelegate.java:163,173,178,183,188`), two STARTED→COMPLETED pairs for the same step could be
  appended before either `evolve` runs (relates to F-1). **F-7 (`modifyPayload`) — FIXED:** previously
  `PayloadDelegate.modifyPayload` was the **only** state-publishing primitive that did **not** gate its publish on
  `!containsStep(stepName)` (it gated only the drift guard, `PayloadDelegate.java:80`), appended its publish task
  **unconditionally** (`:84`), and published the `COMPLETED` step event **directly** via `eventSink.publish` (`:97`)
  instead of through `AbstractStepExecutor.sendStepEvent` — so it **bypassed** the "step already terminal → refuse
  to publish" guard (`AbstractStepExecutor.java:204-212`). When a `modifyPayload` step's `<step>:COMPLETED` committed
  and the process crashed **before** the workflow's `<workflow>:COMPLETED` (instance stays non-terminal), the
  post-crash live re-run re-reached `modifyPayload` with the step present and re-published a **second**
  `<step>:COMPLETED` — a genuine INV-2 violation on a **single node**, deterministic, distinct from the F-1
  split-brain consequence above. **Fixed:** `modifyPayload` now gates **both** the drift guard AND its publish task on
  the single `!containsStep(stepName)` check (`PayloadDelegate.java:83`) — the replay-skip gate every other
  state-publishing primitive has — so the post-crash live re-run finds the step present and **skips** the re-publish
  (the trailing `awaitStateChange` returns the cached COMPLETED immediately), keeping the count at 1 and INV-2 holding.
  Matches `ExecuteDelegate` (gates the whole publish on `!containsStep`, `ExecuteDelegate.java:117`, and routes
  COMPLETED through the guarded `sendStepEvent`), which **skips** the re-publish. See POC-TLA-DST.adoc finding **F-7**
  (FIXED); pinned by `F7DuplicatePayloadRecordTest` / `DuplicateTerminalPayloadRecordScenario` /
  `DuplicatePayloadWorkflow` (seed 0), now asserting the count stays 1 / INV-2 holds across the crash/recover, and by
  TLA+ `PayloadRepublish.tla` (`MC_payload.cfg` violated models the old behaviour, `MC_payload_fixed.cfg` → `No error`).
  **Coverage gap — F-13 (workflow-status duplicates out of scope):** `Invariants.assertAtMostOnceRecording` reads each
  event's *step* status and `continue`s when it is empty, then keys the at-most-once count on
  `getWorkflowId()/getStepName()` — so it counts **only step-status terminals**. A duplicate *workflow-status* terminal
  record (a second `<workflow>:CANCELLED` for one `workflowId`, the F-13 cancel-path duplicate) carries no step status and
  is therefore **never counted by INV-2** — it falls solely to INV-7 (`TerminalIsFinal`). This coverage gap is confirmed
  and documented as part of finding **F-13**. Candidate fix (closing the gap): extend `assertAtMostOnceRecording` to also
  count workflow-status terminal records per `workflowId` (so a duplicate `<workflow>:X` trips INV-2 too, not only INV-7).
- **Checked by:** both — TLA+ invariant (count terminal records per `(w, s)` ≤ 1, including under an
  interleaving where two owners publish; plus `PayloadRepublish.tla` for the F-7 crash-then-re-publish window) + DST
  post-step assertion (after every committed event, assert each step name appears ≤ once as terminal in
  `workflowHistoryRepository` history) + the F-7 single-node regression (`F7DuplicatePayloadRecordTest`:
  `assertAtMostOnceRecording` **holds** on the recovered log — the fixed `modifyPayload` no longer re-records its
  terminal step across a crash/recover).

---

### INV-3: Committed history survives a crash (durability)
- **Name:** `CommittedHistorySurvivesCrash`
- **Kind:** Safety
- **Plain English:** Any workflow event that was durably committed to the event store before a crash
  is still present, in the same order, after recovery — recovery never loses or reorders committed
  history.
- **Formal-ish:** `∀ t : history_committed(beforeCrash=t) ⊑ history(afterRecovery)` (prefix /
  subsequence preservation): for every event `e` with `committed(e) ∧ time(e) < crashTime`,
  `e ∈ store(afterRecovery)` and the relative order of committed events is unchanged. The reset on
  restart goes *back* to the safe point and re-streams; it must not truncate committed events.
- **Engine mapping:** The durable substrate is the `EventStore` (Axon Server DCB in production,
  `BikeRentalIT.kt:48-51`; `InMemoryEventStorageEngine` in tests, §4 table). Recovery does NOT rely
  on the (non-durable) processor token — it rides on the durable `SafePointStore` + full replay from
  the safe point (`WorkflowEngine.java:152-180`; ADR-004). Restart selects the reset token via
  `determineResetToken` (`WorkflowEventProcessingRegistrationEnhancer.java:238-249`) and
  `processor.resetTokens(resetToken)` (`:233-234`) re-streams from there — events are re-read, never
  dropped. Safe point = `lowerBound` over active restart tokens (`WorkflowEngine.java:328`), so the
  reset point is `≤` the oldest active execution's start (ADR-004 consequences: "still reconstructs
  correct state", may replay *more* than necessary but never less).
- **Currently holds?** Yes — assuming the underlying `EventStore` is durable and append-only (true
  for Axon Server DCB; the in-memory engine is durable *within a test process*, which is exactly what
  DST exercises by pre-seeding it, `WorkflowReplayPreparedStateTest.java:162-268`). Not at risk from
  F-0/F-1; it's the property crash-recovery is built on.
  **Cost facet — finding F-11 (`SafePointPinnedByNonTerminalInstance`, liveness / unbounded-retention).** INV-3 is a
  *correctness* property and it HOLDS unconditionally; but the safe point this recovery rides on can be **pinned**
  arbitrarily far behind the log head by a single non-terminal instance, so recovery replays *unboundedly more* than
  necessary (never less — INV-3 is never weakened). The engine safe point is `lowerBound` over the `restartToken()` of
  **every** execution in `workflowExecutionRepository.findAll()` (`WorkflowEngine.determineEngineSafePoint`,
  `WorkflowEngine.java:317-331`), and an execution is removed from that set **only** when it reaches a terminal status
  (the termination callback `remove(...)` + `persistEngineSafePoint()`, `WorkflowEngine.java:194`; the `switchToLiveMode`
  terminal filter, `WorkflowEngine.java:158-163`, filter at `:161`). There is **no** eviction path for a non-terminal
  instance, so an instance parked in a never-arriving `waitForEvent` pins the floor of `lowerBound` forever — on every
  crash/restart `processor.resetTokens(thatToken)` replays the entire log from the pinned early token (ADR-004 "replay ≥
  necessary"), and the instance is retained in memory indefinitely; both grow without bound while it lives. This is the
  **parked complement of F-6/S-4** (whose wedge *throws* out of the body, so the execution IS removed at `:194` and the
  safe point can advance; F-11's instance *parks*, is never evicted, and pins the floor). Characterized + flagged, **not**
  patched (POC rules). Candidate fix: bound retention / a non-terminal-aware safe-point floor / evict-or-checkpoint
  long-parked instances. See POC-TLA-DST.adoc finding **F-11** (and INV-5's parked-wait carve-out below); DST-confirmed
  (seed 0) by `F11SafePointPinnedByNonTerminalTest` (via `SafePointPinnedByNonTerminalScenario`): the safe point stays at
  the parked instance's early token (== its restart token) while N later instances complete + are evicted and the log
  head advances ≥ N past it, and a `crashAndRecover()` resets to that pinned token.
  **Recovery facet — finding F-12 (`LostRecoveryOnShutdownRace`, lost recovery / HIGH).** INV-3's *durability* property
  HOLDS — a committed `<workflow>:STARTED` is still on the durable log after recovery — but the **recovery that rides on
  the safe point can FAIL to re-drive it** when the safe point is advanced **too far forward** by a graceful-shutdown
  race (the opposite over-advance to F-11's pin-too-far-behind). On a graceful `WorkflowEngine.shutdown()`
  (`WorkflowEngine.java:277-286`) each in-flight execution is interrupted; the interrupted body still runs the
  per-execution termination handler (`remove(workflowId)` + `persistEngineSafePoint()`, `:190-196`) **even though the
  instance is still non-terminal (STARTED)** — on the body's virtual thread, so it can land after the repository was
  emptied; `determineEngineSafePoint()`'s empty-repository branch (`:319-320`) then stores `currentTrackingToken.get()` —
  the LATEST token — advanced past the in-flight instance's restart token. On restart `requiresReplay`
  (`WorkflowEventProcessingRegistrationEnhancer.java:252-257`) is then `false` (`safePoint samePositionAs latest`), so the
  engine switches straight to live and the committed `<workflow>:STARTED` is never replayed / re-created / resumed
  (recovery replays *less*, dropping the in-flight instance — also an INV-5 gap, below). **Reachable on a graceful
  shutdown only** (the virtual-thread executor drains the interrupt-driven `finishWorkflow` before exit, wired via
  `WorkflowConfigurationDefaults` `onShutdown(INBOUND_EVENT_CONNECTORS)`); a **hard crash (kill -9) cannot hit it** (the
  process is lost before the async write reaches durable storage). The default DST scenarios do not surface it because
  the shared `SimulationWorld.crashAndRecover()` DELIBERATELY MASKS the race (`crashAndRecoverInternal` `:326-340` freezes
  the safe-point store + re-pins the crash-time token, which also faithfully models a hard crash); characterized + flagged,
  **not** patched (POC rules). Candidate fix: do not store the empty-repo 'latest' safe point while a removed instance was
  non-terminal (advance only on genuinely-terminal removals), or compute the safe point before removing a non-terminal
  instance. See POC-TLA-DST.adoc finding **F-12** (and INV-5 below); DST-confirmed (seed 0) by
  `F12LostRecoveryOnShutdownRaceTest` (via `LostRecoveryOnShutdownRaceScenario`, through the scenario-only unmasked
  `SimulationWorld.crashAndRecoverPersistingEngineSafePoint`): after the unmasked shutdown-race recovery the stored safe
  point == the log head (LATEST) and the in-flight instance is NOT re-created, while its committed `<workflow>:STARTED`
  still survives on the durable log.
- **Checked by:** both — TLA+ invariant (model a crash as dropping volatile state but keeping the
  committed log; assert post-recovery log ⊇ pre-crash committed log) + DST post-step assertion
  (snapshot committed events before an injected crash, restart engine over the same store, assert the
  event log is a superset with order preserved).

---

### INV-4: Deterministic replay
- **Name:** `DeterministicReplay`
- **Kind:** Safety
- **Plain English:** Replaying the same committed history always rebuilds the same instance state and
  drives the same sequence of step decisions — replay never diverges from the run that produced the
  log, and re-running it twice yields the same result.
- **Formal-ish:** `replay(h) = replay(h)` and `state(afterReplay(h)) = state(atCommit(h))` for any
  committed history `h`: the post-replay `Map<String, WorkflowStep>` and the ordered
  `workflowStepNames()` are a pure function of `h` (independent of wall-clock, thread scheduling, and
  arrival interleaving). New live steps may only *append* after the replayed prefix; the replayed
  prefix itself is fixed.
- **Engine mapping:** Replay applies events **synchronously**, single-threaded, with no executor and
  no new side effects: `state().evolve(eventMessage, processingContext)`
  (`SimpleWorkflowExecution.java:362-365`, §5 step 3). Cached-result resumption means the body re-runs
  but absent-step bodies only fire past the recorded prefix (`ExecuteDelegate.java:117`, §5 step 5).
  Retry timing is reconstructed from recorded `step.timestamp()`, not in-memory state
  (`RetryableExecuteDelegate.java:96-104,172-178`), and per-attempt timeouts are measured from the
  recorded STARTED timestamp so they survive replay (`ExecuteDelegate.java:133`). **Threats to
  determinism** (§8 "Ordering caveats", §11): `workflowStepNames()` sorts by `WorkflowStep::timestamp`
  — deterministic only if timestamps are unique (ties → unspecified order,
  `EventSourcedWorkflowState.java:166-172`); `EventWaitConditions.evaluateAndApply` iterates a
  `ConcurrentHashMap.entrySet()` in hash order when one event satisfies multiple waits
  (`EventWaitConditions.java:111`); `findAll()` returns an unordered set iterated in `switchToLiveMode`
  (`WorkflowEngine.java:172`). UoW ids use `UUID.randomUUID()` (`ProcessingContextUtils.java:79`) but
  are not persisted as workflow state.
- **Currently holds?** Partial — **F-2**. The *replayed prefix from the committed log* is
  deterministic by construction (synchronous `evolve`); the at-risk surface is the boundary where
  replay meets live and where ordering is hash/timestamp-derived (timestamp ties; multi-match wait
  dispatch; live-switch race, §13.3). The safe-point *value* itself is deterministic because
  `lowerBound` is commutative (§11). Honest read: deterministic where the log fully orders events;
  potentially non-deterministic on ties and at the live/replay flip. The invariant-expansion campaign
  sharpened the F-2 read: the per-instance **global-append order** is itself non-deterministic — not
  only cross-instance but *intra*-instance for events committed close together (the engine publishes
  durably-async, so a step's terminal record can land just before/after the workflow-completion commit
  run-to-run). So what is deterministic per instance is the committed **content** (which events, with
  what multiplicity), not their global-append order.
- **Checked by:** both — TLA+ invariant (assert the decision/command sequence is a function of the
  history; force timestamp ties and a multi-match wakeup to look for divergence) + DST post-step
  assertion (`assertDeterministicReplay`: replay the same seeded committed log twice under the
  deterministic clock/executor and assert the per-instance committed event **multiset** is identical —
  same step names + statuses, same multiplicity — comparing content rather than the F-2 global-append
  order; flag any divergence as F-2).

---

### INV-5: Every workflow eventually completes (no permanent stall)
- **Name:** `EventuallyTerminates`
- **Kind:** Liveness
- **Plain English:** A started workflow does not get stuck forever: under fair scheduling and given
  its awaited events/timers eventually fire, every instance eventually reaches a terminal status
  (COMPLETED, FAILED, CANCELLED, or TIMED_OUT) — modulo *intended* waits (`waitForEvent`/`sleep`),
  which resolve once their event/timeout arrives.
- **Formal-ish:** `∀ w ∈ Instances : ◇ (status(w) ∈ {COMPLETED, FAILED, CANCELLED, TIMED_OUT})`
  under weak fairness on the per-instance task queue and the assumption that every awaited event is
  eventually delivered and every scheduled timeout eventually fires. Workflow-level terminal statuses
  are exactly those applied at `EventSourcedWorkflowState.java:251-281`.
- **Engine mapping:** Progress carrier = the per-instance single-threaded task queue,
  `awaitStateChange` looping `taskQueue.take(); task.accept(this)` until a predicate holds
  (`SimpleWorkflowExecution.java:346-353`). Timers convert waits into terminal/next-step transitions:
  per-attempt timeout `result.orTimeout(...)` (`ExecuteDelegate.java:167-168`), wait timeout and
  backoff via `CompletableFuture.delayedExecutor(...)` (`WaitForDelegate.java:138`,
  `RetryableExecuteDelegate.java:200-206`); retries bounded by `maxRetries` →
  `attempt <= maxRetries` (`RetryPolicy.java:119-121`, §6). On restart, surviving executions are
  re-driven by `switchToLiveMode` → `execute(...)` (`WorkflowEngine.java:172-174`). **Stall threats:**
  the drift guard pauses an instance **non-terminally** on `WorkflowReplayDriftException` — logs a
  warning and publishes no terminal event, intentionally leaving it stuck pending a corrected redeploy
  (`SimpleWorkflowExecution.java:290-298`, §5, ADR-005 §5) → a *deliberate* non-termination requiring
  human action; the task queue is bounded at 1000 and `appendTask` throws "Too many tasks" with no
  backpressure (`SimpleWorkflowExecution.java:472-477`, §13.4); the timer ForkJoinPool path is not
  injectable, so DST must supply virtual time to make "eventually fires" concrete (§12).
- **Currently holds?** Partial — holds for the normal path under fairness + delivered events/timers;
  **deliberately does NOT hold** for the drift-paused state (by design, awaiting redeploy) and is
  **undefined** under task-queue overflow (§13.4). The model must treat drift-pause as an accepted
  terminal-for-liveness sink or an explicit fairness exclusion, not a bug.
  **Carve-out note — the `handleWorkflowException` `default`-branch wedge is BROADER than the documented
  drift-pause (candidate finding F-6, generalized by S-4).** The drift-pause is the *only* non-termination
  this invariant intends to carve out (a `WorkflowReplayDriftException` logged + left non-terminal awaiting a
  corrected redeploy, `SimpleWorkflowExecution.java:289-297`). But `handleWorkflowException`'s **`default`**
  branch (`SimpleWorkflowExecution.java:311-323`) applies the *same* "log + record no terminal status"
  treatment to **any** unexpected `RuntimeException` propagating out of the workflow body / a primitive's task
  ("we agreed not to drive the workflow to terminal state on any other exception") — e.g. a `modifyPayload`
  modifier lambda (`PayloadDelegate.modifyPayload`'s `payloadModification.apply`, `PayloadDelegate.java:86`,
  re-thrown by the `.join()` at `:98`) or a `VersionDelegate.version` publish task (`VersionDelegate.java:140-146`)
  throwing a plain `RuntimeException`. Such an instance is wedged **non-terminally**, the exception merely
  logged, and a crash + recover does **not** rescue it (the wedged execution is removed from the in-memory
  repository when its body throws, `WorkflowEngine.java:194`, so the orphaned `<workflow>:STARTED` is left
  non-terminal forever — recovery-unsafe). This is an **unintended** liveness stall, broader than the
  drift-pause carve-out — characterized + flagged, **not** patched (POC rules). Candidate fix: drive the
  instance to FAILED on an unexpected runtime exception in the `default` branch instead of silently wedging.
  See POC-TLA-DST.adoc finding **F-6** (generalized by **S-4**); DST-confirmed by
  `Inv19PayloadReducerSemanticsTest.throwingModifierLambda_wedgesTheInstance_candidateFindingF6_S4Generalization`
  (via `PayloadReducerSemanticsScenario.runThrowingModifierEdge` + `ReducerWorkflow.throwingModifier`, seed 0).
  **Retention/replay consequence of a long-lived non-termination — finding F-11
  (`SafePointPinnedByNonTerminalInstance`).** Whether a non-terminal instance is *intended* (a legitimately long-running
  / abandoned `waitForEvent`, the complement of the drift-pause and F-6/S-4 carve-outs) or *unintended* (the F-6/S-4
  wedge that PARKS rather than throws — note the F-6/S-4 wedge above *throws*, so its execution is **removed** at
  `WorkflowEngine.java:194` and does NOT pin), while it stays `active` it **pins the engine safe point** at its early
  restart token forever: `determineEngineSafePoint` is `lowerBound` over **every** live execution's `restartToken()`
  (`WorkflowEngine.java:317-331`) and there is no non-terminal eviction path. The consequence is unbounded replay cost
  (every crash/restart replays the whole log from the pinned token, ADR-004) + unbounded in-memory retention while it
  lives — a liveness/retention cost, **not** an INV-5 termination break (this instance *is* the never-terminating one;
  INV-5 carves out intended waits). INV-3's correctness is untouched (replay ≥ necessary, never less). Cross-ref INV-3
  above; characterized + flagged, **not** patched (POC rules). See POC-TLA-DST.adoc finding **F-11**; DST-confirmed
  (seed 0) by `F11SafePointPinnedByNonTerminalTest` (via `SafePointPinnedByNonTerminalScenario`). Candidate fix: bound
  retention / a non-terminal-aware safe-point floor / evict-or-checkpoint long-parked instances.
  **A non-terminal instance can be silently abandoned on recovery — finding F-12 (`LostRecoveryOnShutdownRace`, lost
  recovery / HIGH).** Distinct from the wedges above (which leave a non-terminal instance *present* and stalled): here a
  graceful-shutdown race makes the in-flight (non-terminal, STARTED) instance **disappear** from recovery entirely. On
  `WorkflowEngine.shutdown()` (`WorkflowEngine.java:277-286`) the instance is interrupted, and the interrupted body still
  runs the termination handler `remove(workflowId)` + `persistEngineSafePoint()` (`:190-196`) **while still non-terminal**;
  on the body's virtual thread that can land after the repository is emptied, so the empty-repository safe-point branch
  (`:319-320`) stores the LATEST token. On restart `requiresReplay` is then false, the engine switches straight to live,
  and the instance is never re-created — so it never reaches a terminal status (an INV-5 gap reached by *dropping* the
  instance, not stalling it), even though its committed `<workflow>:STARTED` survives on the durable log (INV-3 recovery
  facet, above). **Graceful shutdown only** (a hard `kill -9` cannot hit it). Characterized + flagged, **not** patched
  (POC rules). See POC-TLA-DST.adoc finding **F-12**; DST-confirmed (seed 0) by `F12LostRecoveryOnShutdownRaceTest` (via
  `LostRecoveryOnShutdownRaceScenario`, through the scenario-only unmasked
  `SimulationWorld.crashAndRecoverPersistingEngineSafePoint` — the shared masked `crashAndRecover()` neutralizes the
  race). Candidate fix: do not store the empty-repo 'latest' safe point while a removed instance was non-terminal, or
  compute the safe point before removing a non-terminal instance.
- **Checked by:** both — TLA+ **temporal property** `◇[]`/`◇` (under fairness constraints; carve out
  the drift-paused state) + DST: run a scenario to a fixed virtual-time horizon and assert every
  instance reaches a terminal status (a non-terminal instance at horizon is a liveness failure unless
  it is the intended drift-pause).

---

### INV-6: Effect at-most-once (the former F-0 gap — now FIXED)
- **Name:** `EffectAtMostOnce`
- **Kind:** Safety
- **Plain English:** A recorded step's external side effect (its `execute` action) runs at most
  once across the workflow's whole lifetime, including across crashes and replays. **This now holds
  (F-0 fixed):** a crash between the action running and its COMPLETED event committing does **not**
  re-run the effect on replay — the engine refuses to re-execute an in-flight attempt and resolves it
  through the regular error flow instead. (The dual cost of at-most-once: the effect may run 0 or 1
  times — an interrupted attempt may not run at all.)
- **Formal-ish:** `∀ w, ∀ s : Cardinality({ applied(action, w, s) }) ≤ 1` holds. The former failure
  window was a crash with `state[w].steps[s] = STARTED ∧ effectApplied(w,s) ∧ ¬committed(COMPLETED(w,s))`,
  after which replay found the step still STARTED and ran `action.apply` again. The fix snapshots
  "STARTED at the execute entry" — which on a live run is impossible (STARTED is published later) and
  so can only mean a replayed in-flight attempt — and skips the re-run. (Note the contrast with INV-2:
  the COMPLETED *record* was always written at most once; it was the *effect* that used to repeat.)
- **Engine mapping:** The effect is `action.apply(procContext, payload)` (`ExecuteDelegate.java:148`).
  The at-most-once guard sits at the top of the shared `ExecuteDelegate.execute(command, fh, th)`
  overload: it snapshots `containsStep(stepName) && status == STARTED` *before* this run publishes
  STARTED, and when set routes the interrupted attempt through the passed-in `failureHandler` instead
  of re-running the action — **no retry policy → step FAILED** (cause `StepIndeterminateException`);
  **retry policy → RETRYING + next attempt** (the regular error flow). A live retry attempt reaches the
  overload with status RETRYING (never STARTED at entry), so it is unaffected and still executes. The
  resolution publishes via `appendTask` and is awaited via `awaitStateChange`, exactly like every other
  step outcome. Replay's cached-result path (INV-2/§7) still returns *already-COMPLETED* steps
  unchanged. (The older `// FIXME ... append condition` markers point at the stronger *outbox /
  exactly-once* design, a separate future enhancement; at-most-once via skip-and-resolve does not need
  the outbox.)
- **Currently holds?** **Yes — F-0 fixed.** The engine guarantees at-most-once *recording* (INV-2) and
  at-most-once *execution* of effects. A crash-interrupted attempt is not re-run; it surfaces as a
  `StepFailedException` (cause `StepIndeterminateException`) which — by design — the workflow handles
  (`ctx.fail` to terminate, or a retry policy to re-attempt). See `POC-TLA-DST.adoc` F-0 for the
  implementation note + the retry-attempt-resume caveat (strict per-step at-most-once on the no-retry
  path; per-attempt for retried steps).
- **Checked by:** both — TLA+ invariant `EffectAtMostOnce` (`MC_effect_fixed.cfg`, `APPEND_CONDITION=TRUE`
  ⇒ `No error`; `MC_effect.cfg` still **violated**, modelling the *old* at-least-once behaviour) + DST
  post-step assertion `assertEffectAtMostOnce` (≤1, now the live check) exercised by the dedicated
  `WriteThenVanishScenario` / `F0EffectDuplicationTest` (effect == 1, the step resolves to FAILED).

---

### INV-7: Termination is final
- **Name:** `TerminalIsFinal`
- **Kind:** Safety
- **Plain English:** Once a workflow instance records a terminal workflow status (COMPLETED, FAILED, CANCELLED, or
  TIMED_OUT), the engine does no further **work** for that instance — no new step attempt begins and no second
  workflow-status event is recorded; termination is final, even across a crash/replay or the live-switch boundary. (A
  step that finished *before* termination may have its terminal record land in the global log just *after* the
  workflow-completion event because the engine publishes durably-async — see "Currently holds?"; that is a recording-
  order artifact, not new work.) **Documented gap — F-13:** a *re-published identical* terminal workflow status (a
  second `<workflow>:X` for an instance already terminal at `X`, e.g. a second `<workflow>:CANCELLED`) is a duplicate
  durable terminal record the engine's ungated cancel path produces across a crash/replay; it is **tolerated** here as
  the documented F-13 gap (no new lifecycle state is reached) while still flagging a *different* status after terminal.
- **Formal-ish:** for each `workflowId`: no **new-work** event ordered after that instance's terminal-status event.
  Concretely `∀ w ∈ Instances : let t = the first e ∈ history(w) with e.workflowStatus ∈ TerminalWorkflowStatuses, and
  let S = { e.step : e ∈ history(w), ordered(e, before=t) } (steps with any event before t) in ∄ e' ∈ history(w) :
  ordered(e', after=t) ∧ ((e'.workflowStatus ≠ ⊥ ∧ e'.workflowStatus ≠ t.workflowStatus) ∨ (e'.stepStatus ≠ ⊥ ∧
  e'.step ∉ S))` — i.e. a workflow-status event after `t` reaching a **different** status than `t` (a re-published
  *identical* terminal status is the tolerated F-13 gap), or a step event after `t` for a step with **no** event before
  `t` (a step appearing wholly after terminal = new work), is the break. A late-appended event (its STARTED or its terminal record) of a step that **began before**
  `t` is **tolerated**: the engine publishes events durably-async, so a step committed close to the workflow-completion
  commit can have its own commit globally appended just after it (the F-2 append-order non-determinism, intra-instance —
  fuzz seeds 252 = a late COMPLETED, 18 = a late STARTED); the engine's terminal guards forbid *starting* new work after
  `t`, so a step already in `S` cannot be new work. A *re-published identical* terminal workflow status (a second
  `<workflow>:X` with `X` = the terminal status already at `t`) is likewise **tolerated** — the documented F-13 gap: the
  cancel path re-publishes a duplicate `<workflow>:CANCELLED` across a crash/replay, and because the status is identical
  no new lifecycle state is reached (a *different* terminal status after `t` is still the break). **Coverage note:** a
  duplicate *step* terminal record is caught by INV-2 (`AtMostOnceRecording`); a duplicate *workflow-status* terminal
  record is **not** — INV-2 counts only step-status terminals (see INV-2), so a duplicate `<workflow>:X` falls solely to
  this invariant (INV-7). Ordering is taken
  **per `workflowId`** (this instance's own committed subsequence), never by global index — distinct instances interleave
  non-deterministically in the single global log (the F-2 surface, §11), so another instance's event landing after this
  one's terminal status is not a violation; this instance *starting new work* after its own terminal status is.
- **Engine mapping:** Terminal workflow statuses are applied at `EventSourcedWorkflowState.java:251-281` and
  `WorkflowStatus.isTerminal()` enumerates them (`COMPLETED/FAILED/CANCELLED/TIMED_OUT`). `ctx.fail`/`ctx.cancel` end
  the workflow immediately (axon-flow-workflow skill §3.5) by throwing `WorkflowFailedException`/
  `WorkflowCancelledException`, handled in `SimpleWorkflowExecution.handleWorkflowException` (`:233-271`) which
  publishes the single terminal workflow-status event. The *step* publish side enforces finality: `sendStepEvent`
  refuses to publish once the workflow is terminal (`AbstractStepExecutor.java:197-203`) and once a step is terminal
  (`:204-212`); `evolve` ignores transitions on an already-terminal step (`EventSourcedWorkflowState.java:195-199`).
  On replay a present step yields a cached result and emits nothing (`ExecuteDelegate.java:117`, §7), so a re-reached
  `ctx.cancel`/`ctx.fail` on an already-terminal instance is *intended* to be a no-op. **But the *workflow-terminal*
  publish is NOT guarded the same way (F-13):** `ctx.cancel()` → `TerminateDelegate.cancelled` publishes
  `<workflow>:CANCELLED` **directly** via `eventSink.publish(...)` (`TerminateDelegate.java:178`) with no "already
  terminal" gate at the publish site; the only protection on replay is the upstream `execute()` short-circuit
  (`SimpleWorkflowExecution.java:149`) and the `switchToLiveMode` terminal-eviction filter
  (`WorkflowEngine.java:158-163`). When the recovered in-memory state has not yet reflected the committed terminal status
  at filter time, the body is re-driven and re-publishes a **second, identical** `<workflow>:CANCELLED` — a duplicate
  durable workflow-terminal record (an intermittent crash/replay-state race). See POC-TLA-DST.adoc finding **F-13**.
- **Currently holds?** Yes (honest assessment) — the engine's terminal guards on the publish side plus the
  cached-result replay path mean an instance starts no new work after it goes terminal; the DST harness asserts this
  after every step (including across the crash/recover faults) and the dedicated cancel scenario asserts it across a
  crash/replay + start-event redelivery. No counterexample observed across the fuzz campaign. Two fuzz seeds did
  (timing-dependently) surface a tightly-committing instance's own step event globally appended just after its
  workflow-completion event — `252` a late *COMPLETED* (`recordMatch`), `18` a late *STARTED* (`processV2` on the
  migrating instance, whose `COMPLETED` was already committed before the terminal status). The engine publishes
  durably-async, so a step committed close to the completion commit can have its own commit (STARTED or terminal record)
  land just after it (F-2 append-order non-determinism, intra-instance, timing-dependent — not new work, not an engine
  defect: the engine's terminal guards forbid *starting* new work after terminal, so a step that began before terminal
  cannot be new work). The assertion therefore tolerates any late-appended event of a step that began before terminal,
  and flags only genuine new work (a *different* workflow status, or a step with no pre-terminal event); seeds 252 and 18
  are pinned in `RegressionSeedsTest` (see POC-TLA-DST.adoc F-2 note).
  **Documented gap — F-13 (No, for the cancel path):** the engine's *workflow-terminal* publish is **not** gated on
  "already terminal" at the publish site (only the upstream `execute()` short-circuit + the `switchToLiveMode`
  terminal-eviction filter protect it), so across a crash/replay the recovered cancelled body can re-run, re-reach
  `ctx.cancel()`, and re-publish a **second, identical** `<workflow>:CANCELLED` for the same `workflowId` — a duplicate
  durable workflow-terminal record (corruption class; the cancel-path / workflow-terminal analogue of F-7). This is an
  **intermittent** crash/replay-state-timing race (~1-in-4 independent crashes), and is exactly the shape that
  intermittently tripped `Inv7TerminalIsFinalTest.cancelledWorkflow_...`. The same ungated publish also re-publishes the
  duplicate **deterministically** when the body is re-driven by an F-3 start-event redelivery restart after the crash.
  `assertTerminalIsFinal` now **tolerates** a re-published identical terminal status as this documented gap (so the gated
  build is deterministically green while it is open), and the gap is pinned **deterministically** by
  `F13DuplicateCancelRecordTest` / `DuplicateCancelTerminalRecordScenario` (cancel → terminal → shared `crashAndRecover()`
  → count stays 1; then redeliver the start event → the re-driven body re-publishes → `<workflow>:CANCELLED` count == 2).
  **Note the
  INV-2 coverage gap (part of F-13):** INV-2 (`AtMostOnceRecording`) does **not** catch this duplicate — it counts only
  step-status terminals — so a duplicate workflow-status terminal falls solely to INV-7. Candidate fix: gate the
  workflow-terminal publish on "already terminal" (the F-7-class fix), and/or extend `assertAtMostOnceRecording` to count
  workflow-status terminal duplicates too. See POC-TLA-DST.adoc F-13.
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and its `log` is a sequence of
  `<<step, status>>` step records with **no notion of a workflow-level terminal status**; adding workflow-status
  events + a finality operator would introduce a new state concept outside that scope and bloat a model that is kept
  tiny so TLC finishes in seconds. INV-7 is an implementation/DST property (it is about the engine's terminal-guard
  behaviour across replay, not the leasing protocol), so it is asserted in the simulator only —
  `Invariants.assertTerminalIsFinal` (per `workflowId`), exercised by `DstSimulation` (every step) and
  `TerminalIsFinalScenario` (`Inv7TerminalIsFinalTest`).

---

### INV-8: Retry bound
- **Name:** `RetryBound`
- **Kind:** Safety
- **Plain English:** For a step configured with `RetryPolicy.maxRetries(n)`, the number of *attempt records* for that
  `(workflowId, stepName)` in the committed history is at most `n + 1` — the engine never records more attempts than
  the policy allows, even across crashes/replays. (An *attempt record* is a non-terminal step event: the single
  `STARTED` plus each `RETRYING`. This is **record-level**, the deliberate contrast with INV-6/F-0: a crash can make a
  step's *effect* run more than once, but the recorded *attempt count* must stay within the policy bound.)
- **Formal-ish:** `∀ w ∈ Instances, ∀ s ∈ StepNames with a configured maxRetries(n) :
  Cardinality({ e ∈ history(w) : e.step = s ∧ e.stepStatus ∈ {STARTED, RETRYING} }) ≤ n + 1`. Equivalently: at most
  one `STARTED` and at most `n` `RETRYING` records are ever committed for `(w, s)`. Taken **per `(workflowId,
  stepName)`** (never pooled across instances or steps). The cardinality is over the **set** of committed events —
  the DST assertion counts **distinct committed events (by event identifier)**: an at-least-once durable store may
  legitimately hold the SAME committed event twice (the `DUPLICATED_APPEND` fault — a retried append whose first
  attempt landed), and that store-level duplicate is one attempt, not two; a genuine engine re-publish mints a NEW
  event identifier and is still counted (the F-7/F-13/F-20 duplicate-record corruption class stays detected). See
  POC-TLA-DST.adoc finding **F-24**.
- **Engine mapping:** Retries are driven by `RetryableExecuteDelegate`. The single `STARTED` is emitted by
  `ExecuteDelegate` on the first launch, guarded by `!containsStep(stepName)` so a resumed/retried attempt never
  re-emits it (`ExecuteDelegate.java:117-121`). Each retry decision emits one `RETRYING` via
  `retrying(stepName, retryInfo, …)` (`RetryableExecuteDelegate.java:143`), gated by `RetryPolicy.shouldRetry` which is
  bounded by `attempt <= maxRetries` (`RetryPolicy.java:119-121`); on exhaustion the step goes terminal
  (`FAILED`/`TIMED_OUT`) and no further attempt is launched. Across a crash, recovery resumes from the persisted
  `RETRYING` state and schedules the *next* attempt from the recorded `StepRetryInfo.attempt()`
  (`RetryableExecuteDelegate.java:101-107`) rather than restarting the count, and a replayed terminal step yields a
  cached result and emits nothing (`ExecuteDelegate.java:117`) — so the attempt count is bounded across replay too.
- **Currently holds?** Yes (honest assessment) — the engine's `attempt <= maxRetries` gate plus the single-`STARTED`
  guard and the resume-from-persisted-`RETRYING` path keep the committed attempt-record count at `≤ maxRetries + 1`;
  the DST harness asserts this after every step (including across the crash/restart faults) and the dedicated scenario
  asserts it on an always-failing step driven to exhaustion across a crash/replay. No counterexample observed across
  the fuzz campaign.
  **Backoff-arithmetic edge — finding F-9 (S-3), liveness-wedge but EXTREME-config / LOW likelihood — FIXED.** The
  record-level bound above was **never** violated by the `BackoffStrategy.exponential` overflow; the bound *held* (the
  overflow stopped recording *early*, it never recorded *more*). But the bound is record-level only — it was silent about
  the engine getting *stuck before* recording the next attempt. _Was:_ `BackoffStrategy.exponential(base, max)` computed
  `factor = 1L << (attempt - 1); computed = base.multipliedBy(factor); return computed > max ? max : computed;`
  — the cap was applied **after** the multiply, so the multiply could throw or wrap **before** the clamp. `attempt` runs
  up to `maxRetries + 1`, so for a large `maxRetries` (e.g. `maxRetries(70)`) with a seconds/minutes-scale `base`,
  `base.multipliedBy(2^(attempt-1))` **overflowed `Duration`'s capacity and threw `ArithmeticException`** ("Exceeds
  capacity of Duration") around `attempt ≈ 59-63`; for a small base, at `attempt == 64` the shift `1L << 63` was
  `Long.MIN_VALUE` (negative) so a **negative `Duration`** was returned that the `computed > max` cap did NOT clamp (a
  negative is not `> max`), and at `attempt ≥ 65` the JVM masked the shift count mod 64 (`1L << 64 == 1`) so the factor
  **wrapped to tiny positive values** (a non-monotonic garbage schedule). The throw was the headline: `delay(attempt)` is
  computed on the **workflow thread** in `RetryableExecuteDelegate.handleAttemptFailure` (`:136`, inside the
  `FailureHandler` appended task), so the `ArithmeticException` propagated up out of the appended task → the workflow body
  → `handleWorkflowException`'s **`default`** branch (`SimpleWorkflowExecution.java:316-328`) → the instance was **wedged
  non-terminally** (the SAME liveness-wedge sink as F-6/S-4 / the INV-5 carve-out broadening). _Now (fix):_
  `BackoffStrategy.exponential` (`BackoffStrategy.java:72-81`) clamps the shift exponent
  (`1L << min(max(attempt-1,0),62)`) and returns `max` whenever `factor > max/base` (or `base ≤ 0`), so the multiply can
  never overflow, go negative, or wrap — preserving `min(base·2^(attempt-1), max)` for the in-range attempts. The
  always-failing exponential-backoff step now retries on the clamped schedule to exhaustion and goes terminal `FAILED`
  (the workflow reaches a terminal status); INV-8's bound is exact (`maxRetries + 1` attempt records). See POC-TLA-DST.adoc
  finding **F-9**; DST-confirmed (unit-level arithmetic + reachable engine terminal, seed 0) by
  `S3BackoffExponentialOverflowTest` (via `BackoffOverflowScenario` + `BackoffOverflowWorkflow`), flipped from
  documenting the wedge to asserting the fixed clamp-and-exhaust property. Real-world likelihood was **LOW** (needs a
  minute-scale base with a very large `maxRetries`).
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and explicitly does **not** model
  time, timeouts, **retries, or backoff** (see `formal/tla/README.md` "Scope & limits"); its `log` is a sequence of
  `<<step, status>>` records with `status ∈ {STARTED, COMPLETED}` and no `RETRYING`. Adding a retry counter + backoff
  timing would introduce state concepts outside that scope and bloat a model kept tiny so TLC finishes in seconds.
  INV-8 is therefore an implementation/DST property, asserted in the simulator only — `Invariants.assertRetryBound`
  (per `(workflowId, stepName)`), exercised by `DstSimulation` (every step, against `OrderWorkflow`'s genuinely-retrying
  `shipOrder` step) and `Inv8RetryBoundScenario` (`Inv8RetryBoundTest`, an always-failing step to exhaustion). The
  backoff-arithmetic overflow edge (F-9 / S-3) is pinned by the dedicated `S3BackoffExponentialOverflowTest`
  (`BackoffOverflowScenario`), scenario-only (not folded into the fuzz: a deliberately-wedging body leaves an instance
  non-terminal, which the always-on liveness-horizon check would read as a hang).

---

### INV-9: Timeouts fire
- **Name:** `TimeoutsFire`
- **Kind:** Safety
- **Plain English:** A step that exceeds its configured timeout reaches a `TIMED_OUT` outcome (recorded) — a timeout
  never silently hangs or vanishes; the workflow always gets a terminal step outcome it can act on.
- **Formal-ish:** `∀ w ∈ Instances, ∀ s ∈ StepNames with a configured finite timeout(s) :
  let t0 = the committed STARTED timestamp of (w, s) in
  now ≥ t0 + timeout(s) ⇒ ∃ e ∈ history(w) : e.step = s ∧ e.stepStatus ∈ TerminalStepStatuses`. Once the run's
  virtual-time clock `now` has passed `t0 + timeout(s)` the step's window has provably elapsed, so a terminal step
  record must exist (normally `TIMED_OUT`; any terminal status — e.g. a `COMPLETED` that landed just in time — also
  satisfies "did not stay `STARTED` forever"). A step still `STARTED` past its window is the break. Taken **per
  `(workflowId, stepName)`** (never pooled), and only for steps with a configured finite timeout — a step with no
  finite timeout (e.g. `OrderWorkflow`'s effectively-infinite 365-day wait) is unconstrained.
- **Engine mapping:** A wait timeout flows through the injectable `WorkflowScheduler`: `WaitForDelegate` schedules the
  timeout continuation via `scheduler.delayedExecutor(remainingTimeout)` measured from the recorded `STARTED`
  timestamp (`WaitForDelegate.java:138`), and on firing records the step's `TIMED_OUT` event (`timedOut(stepName)`)
  (`StepStatus.TIMED_OUT` per `EventSourcedWorkflowState.java:200-248`). Because the timeout rides the *injectable
  scheduler*, the DST harness drives it fully virtually with `ManualWorkflowScheduler` + `MutableClock` — no JDK
  `orTimeout` wall-clock timer is involved. (The per-attempt `execute` timeout uses a non-injectable `orTimeout`
  residual — ARCHITECTURE.md §12, the adoc's D5 — which is why INV-9 is exercised on the *wait*-timeout path and kept
  out of the per-step fuzz set; see "Checked by".)
- **Surfacing facet (finding F-8 — now FIXED on main, rebase):** All blocking-convenience timeout paths now surface
  `StepTimedOutException` (a subtype of `StepFailedException`): main's fix special-cases `result.timeout()` →
  `StepTimedOutException` in `AbstractDSLWorkflowContext.resolveStepPayload` (and the `SimpleWorkflowContext` helpers),
  symmetric across all overloads, so the asymmetry below is closed. `F8BlockingAwaitTimeoutSurfaceTest` now asserts the
  fixed behaviour (all four paths → `StepTimedOutException`). _The original characterization (pre-fix) is kept below for
  context._ The *recording* contract above (a `TIMED_OUT` step event is
  written once the window elapses) holds on **every** convenience path. The *exception surfaced* by the blocking
  convenience call, before the fix, was **not** uniform: the timeout-surfacing fix that turns a `TIMED_OUT` step into a clean
  `StepTimedOutException` was applied **only** to the **typed** `awaitEvent(stepName, Class, ...)` overload
  (`SimpleWorkflowContext.java:164-190`, which special-cases `result.timeout()`). The `awaitExecute(...)` family (typed
  `awaitExecute(String, Class, Supplier)` delegates to the untyped `awaitExecute(String, Map, processor, customizer)`
  then casts) and the **untyped** `awaitEvent(stepName, EventCondition)` instead route through
  `AbstractDSLWorkflowContext.resolveStepPayload` (`:299-305`: `if (result.success()) {...} throw
  result.error().orElseThrow();`). Because `StateBasedWorkflowStepResult.error()` is **always present** — it maps
  `step.error()` to a `StepFailedException`, and for a `TIMED_OUT` step `WorkflowStep.timedOut(name, payload, ts, ctx)`
  sets the error field to **null** (`WorkflowStep.java:62-64`) — those paths throw `new StepFailedException(null)`: a
  `StepFailedException` wrapping a **null cause**, so the caller cannot tell a timeout from a failure. This is a
  contract/consistency inconsistency (**wrong exception classification, NOT corruption** — minor severity). DST-confirmed
  (seed 0) by the `BlockingAwaitTimeoutSurfaceScenario` (`F8BlockingAwaitTimeoutSurfaceTest`): typed `awaitExecute`,
  untyped `awaitExecute` and untyped `awaitEvent` all surface `StepFailedException(null)` on a timed-out step, while
  typed `awaitEvent` surfaces `StepTimedOutException`. **Candidate fix:** special-case `result.timeout()` →
  `StepTimedOutException` in `resolveStepPayload` / the `awaitExecute` helpers, symmetric with the typed `awaitEvent`
  fix. See `formal/POC-TLA-DST.adoc` Findings → F-8.
- **Currently holds?** Yes (honest assessment) — on the wait-timeout path the engine records `TIMED_OUT` once the
  scheduled continuation fires; the dedicated deterministic scenario drives a `waitForEvent` whose event is never
  delivered past its short timeout (via virtual time) and observes the recorded `TIMED_OUT` step + a terminal workflow,
  and a crash + replay does not re-arm or re-record the already-timed-out step. (If a configured timeout were ever
  found *not* to fire — a step that hangs in `STARTED` past its window — that would be a new finding to triage per the
  POC rules, not silently tolerated.)
- **Checked by:** DST (deterministic scenario only; **not** in the per-step fuzz set due to the `orTimeout` residual —
  Phase-3 D5). **Scope decision (TLA+):** kept DST-only, exactly like INV-7/INV-8. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and explicitly does **not** model
  time or timeouts (see `formal/tla/README.md` "Scope & limits"); adding a clock + timeout transition would introduce
  state concepts outside that scope and bloat a model kept tiny so TLC finishes in seconds. INV-9 is therefore an
  implementation/DST property, asserted in the simulator only — `Invariants.assertTimeoutsFire` (per
  `(workflowId, stepName)`), exercised by the deterministic `Inv9TimeoutsFireScenario` (`Inv9TimeoutsFireTest`); a
  short-timeout `waitForEvent` whose event is never delivered, driven past its window via virtual time. Unlike INV-7
  and INV-8 it is **not** added to `DstSimulation`'s per-step always-on set: the per-attempt `execute` timeout's
  non-injectable `orTimeout` residual makes a fuzz-wide timeout assertion brittle, so INV-9 is pinned by the
  deterministic scenario/test instead.

---

### INV-10: One instance per start
- **Name:** `OneInstancePerStart`
- **Kind:** Safety
- **Plain English:** A single workflow start for a given business key (`workflowId`) yields **exactly one** live
  instance at a time: duplicate/redelivered start events do not create a *second concurrent* live instance. While an
  instance is LIVE the engine dedups a redelivered start (it consults the in-memory spawn-dedup repository and rejects
  the spawn), so at most one non-terminal instance exists per `workflowId`. The complementary case — a start
  redelivered **after** the instance has *terminated* re-spawns the key — is the separate documented finding
  **F-3** (an idempotency / dedup-window design question), **not** an INV-10 violation: INV-10 constrains *concurrent /
  live* duplicates only.
- **Formal-ish:** for each `workflowId`, scanning that instance's own committed subsequence in append order, between
  any two workflow-status `STARTED` events there must be an intervening terminal workflow status. Concretely
  `∀ w ∈ Instances : ∄ i < j : history(w)[i].workflowStatus = STARTED ∧ history(w)[j].workflowStatus = STARTED ∧
  ∄ k ∈ (i, j) : history(w)[k].workflowStatus ∈ TerminalWorkflowStatuses`. Equivalently: a second `STARTED` for an id
  whose prior lifecycle has **not** yet recorded a terminal status (COMPLETED/FAILED/CANCELLED/TIMED_OUT) is the break
  (two concurrent live instances); a second `STARTED` that follows a terminal status is the F-3 after-terminal re-spawn
  and is **tolerated**. Ordering is taken **per `workflowId`** (this instance's own committed subsequence), never by
  global index — distinct instances interleave non-deterministically in the single global log (the F-2 surface, §11),
  so another instance's `STARTED` landing here is irrelevant; this instance opening a second live lifecycle is what is
  forbidden.
- **Engine mapping:** A brand-new start routes through `WorkflowEngine.checkAndCreateNewWorkflow`
  (`WorkflowEngine.java:204-256`), which calls `WorkflowSpawnRouting.resolveWorkflowIdForNewSpawn`
  (`WorkflowSpawnRouting.java:55-92`). That helper does `repository.findById(baseWorkflowId)`
  (`WorkflowSpawnRouting.java:62`): if a **live** instance exists at the same version it returns `null` and logs
  *"already running … treated as a duplicate"* (`:67-76`), so the duplicate start is rejected and **no second live
  instance and no second `<workflow>:STARTED` is appended** — INV-10 holds for the live case. The single `STARTED`
  workflow-status event is published once per spawn (`EventMessageUtils.java:87-89`,
  `MetadataUtils.create(workflowId, WorkflowStatus.STARTED)`). The dedup consults **only** the in-memory
  `WorkflowExecutionRepository`; once an instance terminates it is **evicted** from that repository — on completion
  (`WorkflowEngine.java:194`) and in `switchToLiveMode` (`WorkflowEngine.java:158-163`,
  `findAll().filter(isTerminal).forEach(remove)`, filter at `:161`) — and the dedup never consults the durable event
  log, so a start redelivered *after* termination finds no live instance and spawns a fresh one (a new `STARTED`). That
  after-terminal re-spawn is **F-3**, scoped out of INV-10.
- **Currently holds?** Yes (honest assessment) — for the concurrent/live case the in-memory spawn-dedup rejects a
  duplicate start while the instance is live, so at most one non-terminal instance (and one `STARTED`) per `workflowId`
  exists at a time; the DST harness asserts this after every step (including across the duplicate/redeliver and
  crash/recover faults) and the dedicated scenario delivers a duplicate START while the instance is live and observes a
  single instance. The **after-terminal** re-spawn is the documented exception **F-3** (`formal/POC-TLA-DST.adoc`),
  which INV-10 explicitly tolerates rather than re-flags; a *genuine* second LIVE instance for one id with no
  intervening terminal status (a real dedup failure distinct from F-3) would be a new finding to triage per the POC
  rules, not silently tolerated.
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7/INV-8/INV-9. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to a **single** workflow instance (`Instances = {1}`) over
  leasing + crash-recovery, and its `log` is a sequence of `<<step, status>>` step records with no notion of a
  workflow-level start / spawn-dedup routing across business keys. Modelling spawn dedup would require adding the
  start-event router + a multi-instance-per-key state concept outside that scope and would bloat a model kept tiny so
  TLC finishes in seconds. INV-10 is an implementation/DST property (it is about the engine's in-memory spawn-dedup
  behaviour, not the leasing protocol), so it is asserted in the simulator only — `Invariants.assertOneInstancePerStart`
  (per `workflowId`), exercised by `DstSimulation` (every step) and the deterministic `OneInstancePerStartScenario`
  (`Inv10OneInstancePerStartTest`, seed 0).

---

### INV-11: Version routing is sound
- **Name:** `VersionRoutingSound`
- **Kind:** Safety
- **Plain English:** When multiple versions of a workflow are registered (same `workflowName`/start event/`idProperty`,
  differing by `workflowVersion`), every start/correlated event routes to **exactly one** definition/instance (never 0,
  never 2); a NEW instance spawns at the **highest** registered version; and routing is **deterministic across replay**
  (the same committed history resolves the same version for each instance).
- **Formal-ish:** for each versioned `workflowId` (those of the multi-version workflow): let `versions(w)` =
  `{ e.MessageType.version() : e ∈ history(w) }` (the distinct definition versions stamped on this instance's own
  committed events). Then `∀ w that recorded a STARTED : Cardinality(versions(w)) = 1` (exactly one definition handled
  it — `≥2` = routed to two definitions; for an issued start that produced **no** instance, `0` = dropped/routed to
  none), and `the single v ∈ versions(w) = max(registeredVersions)` (a fresh spawn is at the highest version). The
  deterministic-across-replay facet: `versions(w)` is a pure function of `history(w)`, so `replay(history(w))` yields the
  same resolution — enforced jointly with INV-4 (`DeterministicReplay`), which already proves each instance's committed
  subsequence — including the per-event version stamp — is reproduced verbatim. Taken **per `workflowId`** (never
  pooled): the single global log interleaves independent instances non-deterministically (the F-2 surface, §11), so
  another instance's differently-versioned event landing here is irrelevant; one instance carrying two versions, or an
  issued start producing none, is the break.
- **Engine mapping:** A fresh start routes through `WorkflowEngine.checkAndCreateNewWorkflow`
  (`WorkflowEngine.java:204-256`), which spawns **only** the highest-version configurations:
  `workflowConfigurationRegistry.getHighestVersionConfigurations(...)` (`WorkflowEngine.java:213`;
  `WorkflowConfigurationRegistry.java:74-86` picks the semver-max). The spawn's version is
  `workflowConfiguration.workflowVersion()` (`:227`), which the STARTED event carries on `MessageType.version()`
  (`EventMessageUtils.startedWorkflow` → `versionOf(context)`, `EventMessageUtils.java:88`), and
  `EventSourcedWorkflowState` pins the instance to it (`EventSourcedWorkflowState.java:272-275`, "Pin this instance to
  the version it was started under"). Every subsequent step/status event of the instance is stamped with that same
  version via `versionOf(context)` (`EventMessageUtils.java:193,222,…`). In-flight routing to the matching definition is
  the four-/five-pass lookup `resolveDefinitionForReplay` (`WorkflowConfigurationRegistry.java:249-305`:
  exact-match-spawn-config → exact-match-sibling → closest-sibling ≤ state → closest-higher-sibling > state →
  no-match-fallback); since replay reads the version from the durable started event and the lookup is a pure function of
  it, the resolution is deterministic across replay. Cross-version collisions augment the id with `#<version>`
  (`WorkflowSpawnRouting.java:77`).
- **Currently holds?** Yes (honest assessment) — a fresh start spawns at the highest registered version and every
  committed event of the instance carries exactly that one version; the resolution is deterministic across replay
  (the version lives on the durable started event). The DST harness asserts this after every step (including across the
  crash/restart/reorder faults, against a `VersionedOrderWorkflow` registered at two versions and driven alongside the
  order workhorse) and the dedicated scenario asserts a fresh start picks the highest version, runs the highest-version
  body (its version-only step present, the other version's step absent), resolves to exactly one version, and resolves
  the **same** version after a crash + replay. No counterexample observed across the fuzz campaign. (A genuine break — a
  versioned instance routed to 0 or 2 definitions, or a fresh spawn not at the highest version, or a different version
  resolved across replay — would be a **high-value versioning finding** to triage per the POC rules, not silently
  tolerated.)
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7/INV-8/INV-9/INV-10. The Phase-2 TLA+
  model (`WorkflowLeaseRecovery.tla`) is deliberately scoped to a **single** workflow instance over leasing +
  crash-recovery, with **no notion of workflow versions, a multi-version registry, or start-event→definition routing** —
  its `log` is a sequence of `<<step, status>>` step records with no version stamp and no registry. Modelling version
  routing would require adding versioned definitions + the highest-version spawn rule + the multi-pass replay-routing
  lookup, a whole new state concept outside that scope, and would bloat a model kept tiny so TLC finishes in seconds.
  INV-11 is an implementation/DST property (it is about the engine's multi-version spawn + routing behaviour, not the
  leasing protocol), so it is asserted in the simulator only — `Invariants.assertVersionRoutingSound` (per `workflowId`,
  scoped to the versioned id prefix), exercised by `DstSimulation` (every step, against a two-version
  `VersionedOrderWorkflow` driven alongside the order workhorse through the full fault set) and the deterministic
  `VersionRoutingSoundScenario` (`Inv11VersionRoutingSoundTest`, seed 0).

---

### INV-12: migrateVersion contract
- **Name:** `MigrateVersionContract`
- **Kind:** Safety
- **Plain English:** `ctx.migrateVersion(changeId, v)` obeys its contract across crashes/replays: the recorded
  migration version for a `changeId` is set **at most once** and is **monotonic non-decreasing** (first-writer-wins,
  never downgrades), and **replaying the same history yields the same recorded version** (idempotent — replay does
  not re-apply or change it).
- **Formal-ish:** for each migrating `workflowId` (those of the in-body-migration workflow) and each `changeId`: let
  `markers(w, c)` = `{ e ∈ history(w) : isMigrationStep(e) ∧ e.versionChangeId = c }` (this instance's own committed
  migration-marker events for that change). Then `∀ w, c : Cardinality(markers(w, c)) ≤ 1` (recorded at most once — a
  second marker for the same change means replay re-applied it), and scanning one instance's marker events in committed
  (append) order their recorded `version` values are **non-decreasing** (`∀ i < j : version(marker_i) ≤
  version(marker_j)` — a recorded downgrade is the break), and every recorded `version` passes `Version.validate`. The
  replay-stability facet: `markers(w, c)` and its recorded version are a pure function of `history(w)`, so
  `replay(history(w))` resolves the same recorded version as `atCommit(history(w))` — enforced jointly with INV-4
  (`DeterministicReplay`), which already proves each instance's committed subsequence (including the migration marker)
  is reproduced verbatim. Taken **per `workflowId`** (never pooled): the single global log interleaves independent
  instances non-deterministically (the F-2 surface, §11), so another instance's marker landing here is irrelevant; one
  instance recording the same `changeId` twice, recording a different/downgraded version, or recording an invalid
  version is the break. `markers(w, c)` is a **set** of committed events — the DST assertion counts **distinct
  committed events (by event identifier)**: an at-least-once durable store may legitimately hold the SAME committed
  marker twice (the `DUPLICATED_APPEND` fault), and that store-level duplicate is one recorded marker, not a re-apply;
  a genuine engine re-publish mints a NEW event identifier and is still counted. See POC-TLA-DST.adoc finding
  **F-24**.
- **Engine mapping:** `ctx.migrateVersion(changeId, newVersion)` runs through `VersionDelegate.version`
  (`VersionDelegate.java:96-159`): it returns the recorded value deterministically when the step is already present
  (`state.hasMigrateVersionStep(changeId)`, `:103`); rejects a downgrade (requested not strictly greater than current)
  with `IllegalArgumentException` (`:117-122`); and otherwise publishes a single migration step
  (`EventMessageUtils.migrationStep`, `:138`) — a `COMPLETED` step event with `stepName = changeId` carrying the
  `versionChangeId` + `version` metadata keys (`MetadataUtils.createMigrationStep`). `EventSourcedWorkflowState.evolve`
  applies it first-writer-wins (`versions.putIfAbsent(changeId, version)`, `EventSourcedWorkflowState.java:293`) and
  bumps `workflowDefinitionVersion` only if strictly greater (`:295-297`); a present terminal step is ignored on replay
  (`:195-199`) and a re-reached `migrateVersion` returns the recorded value without emitting (`VersionDelegate.java:103`),
  so replay re-runs the body but appends no second marker. `newVersion` must pass `Version.validate` (semver) before it
  is recorded (`Version.of`, the value the marker carries). The DSL contract (axon-flow-workflow skill §3.7;
  `BaseWorkflowContext.migrateVersion` Javadoc): call at most once per `changeId` per body invocation, never in loops/
  combinators/non-deterministic branches; `changeId` non-blank + stable forever; downgrades throw.
- **Currently holds?** Yes (honest assessment) — the delegate's already-recorded guard + `evolve`'s terminal-step guard
  make a replayed `migrateVersion` a no-op (recorded once, never re-applied), the strictly-greater bump rule keeps the
  recorded version monotonic, and the recorded version lives on the durable marker so it resolves identically across
  replay. The DST harness asserts this after every step (including across the crash/restart/reorder faults, against a
  `MigratingOrderWorkflow` whose body calls `ctx.migrateVersion` once, driven alongside the order workhorse) and the
  dedicated scenario asserts a fresh start records the marker exactly once at the migrated version, takes the migrated
  branch, and resolves the **same** recorded version (unchanged marker count) after a crash + replay. No counterexample
  observed across the fuzz campaign. (A genuine break — a migration recorded twice, a recorded downgrade, or a different
  version resolved across replay — would be a **high-value versioning finding** to triage per the POC rules, not
  silently tolerated.)
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7..11. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery with **no notion of workflow
  versions, a version map, or the `migrateVersion` primitive** (`formal/tla/README.md` "Scope & limits" notes no version
  drift is modelled); its `log` is a sequence of `<<step, status>>` records with no version stamp. Modelling the
  migration record would require adding a version map + the first-writer-wins/strictly-greater apply rule + a downgrade
  rejection, a new state concept outside that scope, bloating a model kept tiny so TLC finishes in seconds. INV-12 is an
  implementation/DST property (it is about the engine's `migrateVersion` recording behaviour across replay, not the
  leasing protocol), so it is asserted in the simulator only — `Invariants.assertMigrateVersionContract` (per
  `workflowId`, scoped to the migrating id prefix), exercised by `DstSimulation` (every step, against a
  `MigratingOrderWorkflow` driven alongside the order workhorse through the full fault set) and the deterministic
  `MigrateVersionContractScenario` (`Inv12MigrateVersionContractTest`, seed 0).

---

### INV-13: No lost payload writes
- **Name:** `NoLostPayloadWrites`
- **Kind:** Safety
- **Plain English:** For a workflow instance, the final committed payload reflects **every** committed step's recorded
  contribution — no committed step's payload write is lost or silently dropped across crashes/replays (modulo intended
  overwrites by a later step on the same key); replaying the same history rebuilds the identical payload.
- **Formal-ish:** for each `workflowId`: let the instance's committed events define, in append order, a sequence of
  payload contributions — a COMPLETED step event carrying a `payloadReducer` (the `modifyPayload` metadata key) names
  the reducer the engine applies (`EventSourcedWorkflowState.evolvePayload`, `:307-335`) with the event's payload map as
  the step's local contribution: `combine_local_and_global` merges each `k=v` into the payload (each is a contribution),
  `local_only` replaces the whole payload with the event's map (each `k=v` is a contribution and the replace is itself an
  intended overwrite), `global_only` discards the result (no contribution). Then for every committed contribution `(k,v)`
  at position `i`, the instance's final payload must contain `k=v` **unless** ∃ a later committed step `j>i` of the same
  instance that wrote key `k` (a later `combine` whose map contains `k`, or any later `local_only` replace — a
  whole-payload rewrite that decides `k`'s fate). A committed contribution neither present in the final payload nor
  overwritten by a later same-key write was **lost** — the break. The replay-stability facet: the final payload is a pure
  function of `history(w)`, so `replay(history(w))` rebuilds the identical payload — enforced jointly with INV-4
  (`DeterministicReplay`), which already proves each instance's committed subsequence (including each payload-bearing
  event) is reproduced verbatim. Taken **per `workflowId`** (never pooled): the single global log interleaves independent
  instances non-deterministically (the F-2 surface, §11), so another instance's payload write landing here is irrelevant;
  one instance losing one of its own committed contributions is the break.
- **Engine mapping:** A step result merges into the payload when the step's `resultPayloadReducer` is
  `CombineGlobalAndLocalPayloadReducer.INSTANCE`; the COMPLETED event then carries that reducer's name on the
  `modifyPayload` metadata key (`ExecuteDelegate.java:174` `completed(stepName, r, resultPayloadReducer.name(), …)`;
  `EventMessageUtils.completedStep` writes the key, `:219-221`). `setPayload`/`modifyPayload` publish a COMPLETED step
  event carrying the new whole payload under the `local_only` reducer (`PayloadDelegate.java:87-91`,
  `LocalOnlyPayloadReducer` replaces). The default `execute` reducer is `GlobalOnlyPayloadReducer`
  (`GlobalOnlyPayloadReducer` returns the global unchanged, discarding the step result — axon-flow-workflow skill §5).
  `evolve` applies the reducer named in metadata on every COMPLETED step (and on the workflow STARTED event, which seeds
  the initial payload under `combine_local_and_global` — `EventMessageUtils.startedWorkflow`, `:88-91`), rebuilding the
  payload deterministically from the committed log (`EventSourcedWorkflowState.java:226-233,272-273,307-335`). Because the
  workflow body must be deterministic (axon-flow-workflow skill §3.3), a replay re-runs the body and rebuilds the same
  payload from the same recorded events; a present terminal step yields a cached result and emits nothing
  (`ExecuteDelegate.java:117`), so no contribution is re-applied or dropped.
- **Currently holds?** Yes (honest assessment) — payload is rebuilt by `evolve` from the committed COMPLETED step events
  via the named reducers; a deterministic body + the cached-result replay path mean a replay rebuilds the identical
  payload, so every committed contribution survives into the final payload (modulo a later same-key overwrite). The DST
  harness asserts this after every step (including across the crash/restart/reorder faults, against a
  `PayloadOrderWorkflow` whose every step writes a distinct payload key) by cross-checking the committed-log payload
  contributions against the engine's **own** reconstructed payload (the workflow-history read-model, built by an
  independent projector), and the dedicated scenario asserts a fresh start's final payload carries every step's
  contribution and a crash + replay rebuilds the identical payload. No counterexample observed across the fuzz campaign.
  (A genuine break — a committed payload write absent from the engine's reconstructed/replayed payload with no later
  same-key overwrite — would be a new finding to triage per the POC rules, not silently tolerated.)
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7..12. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and has **no notion of a workflow
  payload, payload reducers, or `setPayload`/`modifyPayload`** — its `log` is a sequence of `<<step, status>>` step
  records with no payload map (`formal/tla/README.md` "Scope & limits"). Modelling payload evolution would require adding
  a payload map + the three reducer behaviours (merge / replace / discard) + the per-key last-writer reasoning, a whole
  new state concept outside that scope, bloating a model kept tiny so TLC finishes in seconds. INV-13 is an
  implementation/DST property (it is about the engine's payload-evolution behaviour across replay, not the leasing
  protocol), so it is asserted in the simulator only — `Invariants.assertNoLostPayloadWrites` (per `workflowId`, scoped to
  the payload id prefix), exercised by `DstSimulation` (every step, against a `PayloadOrderWorkflow` driven alongside the
  order workhorse through the full fault set) and the deterministic `NoLostPayloadWritesScenario`
  (`Inv13NoLostPayloadWritesTest`, seed 0).

---

### INV-14: Combinator consistency
- **Name:** `CombinatorConsistency`
- **Kind:** Safety
- **Plain English:** A combinator's decision is consistent with the documented short-circuit semantics and is a **pure
  function of its branch steps' committed terminal outcomes** — stable across crash/replay. For a workflow that runs N
  parallel `execute` branch steps and then a combinator over them: `anyMatch` (race / first-match-wins) proceeds on the
  FIRST branch (in declaration order) whose committed outcome satisfies the predicate and is "matched" iff ≥1 branch
  satisfied it; `allMatch` (guard / all-or-first-fail) is "matched" iff ALL branches satisfied it, short-circuiting on
  the first non-matching branch; `noneMatch` (fail-fast) is "matched" iff NO branch satisfied it. The combinator's
  observable downstream effect (which post-combinator step the body recorded, and the payload key it wrote capturing
  matched/unmatched) MUST equal what those semantics dictate given the branches' committed terminal outcomes, and MUST
  be unchanged after a crash + replay.
- **Formal-ish:** for each combinator `workflowId` (those of the combinator workflow) let `branches(w)` =
  `{ (s, sat_s) : s a branch step of w, sat_s = predicate(terminal_outcome(s)) }` (each branch's committed terminal
  outcome, evaluated under the workflow's predicate). Then the recorded decision of each combinator equals the
  semantics-mandated one: `recorded_any(w) = (∃ s ∈ branches(w) : sat_s)`,
  `recorded_all(w) = (∀ s ∈ branches(w) : sat_s)`, `recorded_none(w) = (∄ s ∈ branches(w) : sat_s)`; and, for
  `anyMatch`, the recorded winner is the first branch in declaration order with `sat_s`. The replay-stability facet:
  each recorded decision is a pure function of `history(w)`, so `replay(history(w))` resolves the same decision —
  enforced jointly with INV-4 (`DeterministicReplay`), which already proves each instance's committed subsequence
  (including the branch outcomes and the recorded decision) is reproduced verbatim. Taken **per `workflowId`** (never
  pooled): the single global log interleaves independent instances non-deterministically (the F-2 surface, §11), so
  another instance's combinator step landing here is irrelevant; one instance recording a combinator decision
  inconsistent with its own branch outcomes is the break.
- **Engine mapping:** The combinators are `ctx.anyMatch`/`ctx.allMatch`/`ctx.noneMatch` over `WorkflowStepResult`
  handles, returning a `CombinatorWorkflowStepResult` (`AbstractDSLWorkflowContext.java:227-247`;
  `AnyMatchCombinatorDelegate`/`AllMatchCombinatorDelegate`/`NoneMatchCombinatorDelegate`). The result's `success()` is
  the decision: `anyMatch.success()` ⟺ ≥1 match (delegating to the winner — the first matched in declaration/timestamp
  order), `allMatch.success()` ⟺ all matched (short-circuits on the first non-match), `noneMatch.success()` ⟺ no match
  (short-circuits on the first match) — see `AllMatchCombinator`/`NoneMatchCombinator`/`AnyMatchCombinator` Javadoc and
  the `CombinatorWorkflowStepResult` `matched()`/`unmatched()` contract (read only after `.await()`). The combinator runs
  purely in the workflow body over the branches' already-recorded terminal step results; replay re-runs the body and a
  present terminal step yields a cached result and emits nothing (`ExecuteDelegate.java:117`), so the same handles feed
  the combinator and it resolves the same decision. The decision is made durable by the body recording a distinct
  post-combinator `execute` step (its name encodes matched/unmatched) and a payload key under
  `CombineGlobalAndLocalPayloadReducer` (so the engine's reconstructed payload — `EventSourcedWorkflowState.evolvePayload`
  — carries it).
- **Currently holds?** Yes (honest assessment) — the combinator decision is computed by the runtime as a pure function
  of the branches' terminal results, and because the workflow body is deterministic and a replayed terminal step yields
  a cached result, replaying the same history feeds the combinator the same branch outcomes and it resolves the same
  decision; the recorded post-combinator step and payload key therefore stay consistent with the branches' committed
  outcomes across crash/replay. The DST harness asserts this after every step (including across the
  crash/restart/reorder faults, against a `CombinatorWorkflow` whose three branches vote A=yes, B=no, C=no) by
  re-deriving each combinator's expected decision from the committed branch outcomes and cross-checking it against the
  recorded decision (the distinct post-combinator step name + the engine's reconstructed-payload boolean — two
  independent sources), and the dedicated scenario asserts a fresh start resolves `anyMatch`=matched, `allMatch`=
  unmatched, `noneMatch`=unmatched and a crash + replay resolves the identical decisions. No counterexample observed
  across the fuzz campaign. (A genuine break — a combinator resolving a decision its branches' committed outcomes do not
  support, short-circuiting incorrectly, or rebuilding a different decision on replay — would be a new finding to triage
  per the POC rules, not silently tolerated.)
- **Surfacing facet (sharpened — finding F-10 (S-5), minor): the `anyMatch` winner-derived accessors on the
  no-predicate-match-but-all-branches-completed path read `results[0]`, not "unmatched".** The DECISION facet above (the
  `matched()`/`unmatched()` categorization — the documented way to read a combinator result) is **correct** on this path:
  for `anyMatch(predicate, a, b, c)` where all branches completed but NONE satisfies `predicate`, `matched()` is **empty**
  and `unmatched()` holds all three branches — consistent with the semantics. But the **winner-derived accessors**
  (`success()`/`result()`/`resultAs()`/`error()`, which the `CombinatorWorkflowStepResult` Javadoc documents as delegating
  to "the first element of `matched()`") have **no matched element** to delegate to, so
  `AnyMatchCombinatorDelegate.resolveWinner()` falls into its all-completed branch
  (`AnyMatchCombinatorDelegate.java:83-89`, and the await path `:114-116`) and sets `fallback.orElse(results[0])` — the
  **first completed branch** (declaration order here) — as the winner. Consequently `success()` returns `true` (it reads
  that branch's COMPLETED *step status*, NOT a predicate match) and `result()`/`resultAs()` return that branch's payload,
  on a path where **no branch matched**. An author who reads `anyMatch(...).result()` (or `.success()`) on the
  no-match-all-completed path silently reads branch `a`'s data as if it were "the" result/winner. This is a contract gap
  in the *winner-result semantics* (NOT corruption, NOT a decision inconsistency — minor severity; it only bites if the
  winner accessors are read instead of `matched()`/`unmatched()`). DST-confirmed (seed 0) by the
  `AnyMatchNoMatchWinnerScenario` (`S5AnyMatchNoMatchWinnerTest`): three branches all complete voting `no`,
  `anyMatch.matched()` is empty (correct) yet `success()==true` and `result()` returns branch A's payload. **Candidate
  fix:** make the winner empty / the result accessors `Optional.empty()` (and `success()==false`) explicitly on the
  no-match path, rather than silently returning `results[0]`. See POC-TLA-DST.adoc Findings → F-10.
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7..13. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and has **no notion of combinators,
  parallel branch steps, a predicate over step results, or the `matched()`/`unmatched()` categorization** — its `log`
  is a sequence of `<<step, status>>` step records with `status ∈ {STARTED, COMPLETED}`, no parallel-step fan-out and
  no combinator decision (`formal/tla/README.md` "Scope & limits"). Modelling combinator consistency would require
  adding parallel branch steps + a predicate + the three short-circuit decision rules, a whole new state concept outside
  that scope, bloating a model kept tiny so TLC finishes in seconds. INV-14 is an implementation/DST property (it is
  about the engine's combinator decision behaviour across replay, not the leasing protocol), so it is asserted in the
  simulator only — `Invariants.assertCombinatorConsistency` (per `workflowId`, scoped to the combinator id prefix),
  exercised by `DstSimulation` (every step, against a `CombinatorWorkflow` driven alongside the order workhorse through
  the full fault set) and the deterministic `CombinatorConsistencyScenario` (`Inv14CombinatorConsistencyTest`, seed 0).
  The `anyMatch` no-match-all-completed winner-result edge (F-10 / S-5) is pinned by the dedicated
  `AnyMatchNoMatchWinnerScenario` (`S5AnyMatchNoMatchWinnerTest`), scenario-only (not folded into the fuzz: the
  predicate-level decision the always-on `assertCombinatorConsistency` checks is itself correct on that path, so the
  dedicated characterizing test owns the winner-accessor edge).

---

### INV-15: Event correlation is exact
- **Name:** `EventCorrelationExact`
- **Kind:** Safety
- **Plain English:** An `associate(...)`-correlated event wakes **exactly** the matching waiting instance — never a
  non-matching one (no cross-wakeup) — and duplicate/uncorrelated events produce no spurious wait completion. Concretely,
  for a workflow whose `waitForEvent` step correlates on a per-instance key: an instance's wait step transitions to
  COMPLETED only on delivery of an event whose correlation key equals THAT instance's key; an instance's recorded wait
  completion must never be attributable to a foreign key; an uncorrelated event (no matching waiter) creates no wait
  completion / no spurious step record; a duplicate of an already-matched event does not produce a second wait completion
  (the wait completes at most once for the right key — the correlation facet of at-most-once recording). This is the
  property the axon-flow-workflow skill §10 anti-pattern warns about ("`awaitEvent` without an `associate(...)`
  correlation — an event wakes EVERY waiting workflow"): with the correlation present, an associated event must NOT wake
  every waiter.
- **Formal-ish:** for each correlated `workflowId` (those of the correlated-wait workflow): let `ownKey(w)` be the
  per-instance key its `waitForEvent` associates on (committed by its setup step), and — once its wait completes —
  `matchedKey(w)` the key of the event that woke it (committed by the post-wait `recordMatch` step from the matched
  event's key). Then `∀ w that recorded a wait completion : matchedKey(w) = ownKey(w)` (a `matchedKey ≠ ownKey` is a
  cross-wakeup — an event correlated to another instance's key woke `w`), and `Cardinality({ e ∈ history(w) :
  e.step = awaitSignal ∧ e.stepStatus ∈ TerminalStepStatuses }) ≤ 1` (the wait completes at most once for the right key —
  a duplicate of the matched event must not re-complete it). An instance whose matching key was never delivered (or only
  an uncorrelated key was) records **no** wait completion at all (no spurious completion). The replay-stability facet:
  `matchedKey(w)` is a pure function of `history(w)`, so `replay(history(w))` resolves the same matched key — enforced
  jointly with INV-4 (`DeterministicReplay`), which already proves each instance's committed subsequence (including the
  recorded matched key) is reproduced verbatim. Taken **per `workflowId`** (never pooled): the single global log
  interleaves independent instances non-deterministically (the F-2 surface, §11), so another instance's wait completion
  landing here is irrelevant; one instance recording a foreign matched key, or completing its wait twice, is the break.
- **Engine mapping:** A correlated wait flows through `WaitForDelegate`: `ctx.awaitEvent(step, EventType.class,
  associate(payloadProperty("key"), equalsTo(ownKey)), …)` registers an `EventWaitCondition` keyed by the association
  (the same pattern `OrderWorkflow.awaitConfirmation` uses on `orderId`, axon-flow-workflow skill §4 / §7.5). Incoming
  events are routed by `EventWaitConditions.evaluateAndApply` (`EventWaitConditions.java:111`), which fires **only** the
  waits whose association the event satisfies — so an event whose `key` does not equal an instance's `ownKey` does not
  wake it (no cross-wakeup), and an event with no matching waiter fires nobody. The wait's COMPLETED record is published
  once and the publish-side terminal guard (`AbstractStepExecutor.sendStepEvent` refuses to publish once a step is
  terminal, `:204-212`; the `WaitForDelegate` callbacks re-check `!status().isTerminal()`, `:115,131,150`) plus the
  step-name-keyed dedup keep a duplicate of the matched event from completing the wait a second time. On replay a present
  terminal wait yields a cached result and emits nothing (`ExecuteDelegate.java:117`, §7), so the recorded matched key is
  byte-for-byte stable across replay.
- **Currently holds?** Yes (honest assessment) — the engine's association-keyed wait dispatch wakes only the matching
  waiter, and the publish-side terminal guard + cached-result replay path keep the wait at one completion for the right
  key. The DST harness asserts this after every step (including across the crash/restart/reorder faults, against two
  `CorrelatedWaitWorkflow` instances waiting on distinct keys A and B whose matching `CorrelatedSignalEvent` rides the
  same MESSAGE_REORDER reorder/delay/duplicate path as the order confirmations) by cross-checking each instance's
  recorded `matchedKey` against its own `ownKey` (committed log + the engine's reconstructed payload — two independent
  sources), and the dedicated cross-wakeup scenario delivers A's signal (only A wakes; B stays waiting), an uncorrelated
  key (nobody wakes), a duplicate of A (no second completion), then B's signal (B wakes), then a crash + replay (matched
  keys unchanged). No counterexample observed across the fuzz campaign. (A genuine break — an associated event waking a
  non-matching waiter, or a duplicate completing a wait twice — would be a **high-value finding** (a real engine
  cross-wakeup), to be triaged per the POC rules, not silently tolerated.)
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7..14. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and has **no notion of event
  correlations, associations, a `waitForEvent` association key, or multi-instance event routing** — its `log` is a
  sequence of `<<step, status>>` step records with no association map and no correlated event delivery
  (`formal/tla/README.md` "Scope & limits"). Modelling exact correlation would require adding per-instance association
  keys + an event-routing rule that fires only matching waiters + a duplicate-delivery transition, a whole new state
  concept outside that scope, bloating a model kept tiny so TLC finishes in seconds. INV-15 is an implementation/DST
  property (it is about the engine's association-keyed wait dispatch across replay, not the leasing protocol), so it is
  asserted in the simulator only — `Invariants.assertEventCorrelationExact` (per `workflowId`, scoped to the correlated
  id prefix), exercised by `DstSimulation` (every step, against two `CorrelatedWaitWorkflow` instances waiting on distinct
  keys driven alongside the order workhorse through the full fault set) and the deterministic
  `EventCorrelationExactScenario` (`Inv15EventCorrelationExactTest`, seed 0).

---

### INV-16: Failure propagation
- **Name:** `FailurePropagation`
- **Kind:** Safety
- **Plain English:** A step failure propagates to a terminal **FAILED** workflow status — the workflow never silently
  hangs or completes when a step fails. Concretely, for a workflow whose step throws an uncaught exception (with no or
  exhausted retries): the failing step is recorded with a terminal failure status (FAILED or TIMED_OUT), the instance
  reaches a terminal **FAILED** workflow status (not COMPLETED, not stuck non-terminal at the horizon), and —
  complementing INV-7 — no step after the failing one begins. This is the deliberate contrast with INV-7 (which covers
  `ctx.cancel`→CANCELLED finality) and INV-8 (which bounds retry *attempt records*): INV-16 asserts the FAILURE actually
  *propagates* to a FAILED terminus deterministically, across crash/replay.
- **Formal-ish:** for each failing `workflowId` (those of the failing workflow, scoped to the `fail-`/`failretry-`
  prefixes) that has reached a terminal workflow status: let `failStep(w)` be the failing step. Then
  `terminalWorkflowStatus(w) = FAILED` (a `COMPLETED` here = a step failure that silently completed the workflow — the
  break; a non-terminal instance at the horizon = a silent hang — the break, caught by INV-5 liveness), and
  `∃ e ∈ history(w) : e.step = failStep(w) ∧ e.stepStatus ∈ {FAILED, TIMED_OUT}` (the failure is recorded on the step,
  it did not silently vanish), and `∄ e ∈ history(w) : e.step = afterFailStep(w)` (no step *after* the failing one began
  — the finality facet, jointly with INV-7). The replay-stability facet: the failing-step record and the FAILED workflow
  status are a pure function of `history(w)`, so `replay(history(w))` re-reaches the already-FAILED primitive as a no-op
  and the FAILED terminus is stable — enforced jointly with INV-4 (`DeterministicReplay`). Taken **per `workflowId`**
  (never pooled): the single global log interleaves independent instances non-deterministically (the F-2 surface, §11),
  so another instance's event landing here is irrelevant; this instance failing to propagate its own step failure to a
  FAILED terminus is the break. Asserted on **content** (presence/count of events and the terminal statuses reached),
  **not** the global-append order between this instance's own events — the failing-step terminal record and the
  workflow-FAILED commit can land in either order run-to-run (the F-2 intra-instance non-determinism, §11), harmlessly,
  because both are required present, not ordered.
- **Engine mapping:** A step that always throws records a terminal `FAILED` step status (the apply lambda throws →
  `ExecuteDelegate`'s `failureHandler.onFailure` records `FAILED`, `:191-192`; under a retry policy the bound is reached
  first, then the terminal `FAILED`). The engine does **not** auto-fail the workflow on an uncaught `StepFailedException`
  re-thrown from `awaitExecute` — `SimpleWorkflowExecution.handleWorkflowException`'s `default` branch only **logs** it,
  leaving the workflow non-terminal (`:311-321`, the commented-out failed-workflow publish). So a workflow that intends
  to FAIL on a step failure propagates it explicitly via the terminal primitive `ctx.fail(cause)` (axon-flow-workflow
  skill §3.5; `BaseWorkflowContext.fail`, `TerminateDelegate` → `failedWorkflow(...)` →
  `SimpleWorkflowExecution.handleWorkflowException` `WorkflowFailedException` branch, `:236-253`, publishing the single
  terminal FAILED workflow-status event). `ctx.fail` ends the workflow immediately (§3.5), so no step after it begins; on
  replay the already-FAILED instance re-reaches `ctx.fail` as a no-op (a present terminal step yields a cached result and
  emits nothing, `ExecuteDelegate.java:117`; the publish-side terminal guard refuses a second workflow-status event,
  `AbstractStepExecutor.java:197-203`), so the FAILED terminus survives crash/replay. This mirrors `examples/simple`'s
  `FailWorkflow` (reaches `WorkflowStatus.FAILED` via `ctx.fail`).
- **Currently holds?** Yes (honest assessment) — a step that throws records a terminal `FAILED` step status, the body
  propagates it via `ctx.fail`, and the engine publishes the single terminal FAILED workflow-status event; the
  cached-result replay path + the publish-side terminal guard keep the FAILED terminus stable across crash/replay, and
  `ctx.fail`'s immediate termination means no step after the failing one begins. The dedicated scenario drives both
  failure paths (a no-retry uncaught exception → immediate FAILED, and a retry-exhausting step → FAILED) to a terminal
  FAILED status and asserts the failing step is recorded terminally-failed, the workflow reached FAILED, no post-failure
  step ran, and a crash + replay leaves the FAILED terminus byte-for-byte stable. No counterexample observed across the
  fuzz campaign (the property is exercised through the dedicated deterministic scenario; see "Checked by"). **Note (not a
  new finding, an engine design choice the invariant pins):** an uncaught `StepFailedException` re-thrown from
  `awaitExecute` does **not** by itself drive the workflow to FAILED — it is only logged (`SimpleWorkflowExecution.java`
  `default` branch), so a step failure reaches a terminal FAILED status only when the body propagates it (via `ctx.fail`)
  or via an explicit terminal primitive. INV-16 asserts the propagated path holds; a workflow that throws but never
  propagates would hang non-terminal, which INV-5 (`EventuallyTerminates`) would catch. (A genuine break — a propagated
  step failure that silently completed the workflow, or did not reach FAILED — would be a high-value finding, to be
  triaged per the POC rules, not silently tolerated.) **This is exactly the F-6/S-4 surface (candidate finding):** the
  `default`-branch "log, don't terminate" is correct only for the drift-pause; for *any other* unexpected
  `RuntimeException` from a between-primitives user lambda (e.g. a throwing `modifyPayload` modifier — S-4) it wedges the
  instance non-terminally instead of propagating to FAILED. INV-16 documents that the engine only propagates to FAILED on
  an explicit `ctx.fail`; F-6/S-4 flags the *non-`ctx.fail`* runtime-exception case as the candidate gap (drive it to
  FAILED in the `default` branch). See POC-TLA-DST.adoc finding **F-6** (generalized by **S-4**) and the INV-5 carve-out
  note.
- **Checked by:** DST (deterministic scenario only; **not** in the per-step fuzz set — Phase-3 scenario-pinned, exactly
  like INV-9 `TimeoutsFire`). **Scope decision (TLA+):** kept DST-only, exactly like INV-7..15. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and its `log` is a sequence of
  `<<step, status>>` step records with `status ∈ {STARTED, COMPLETED}` and **no notion of a workflow-level FAILED status,
  a step-failure status, or the `ctx.fail` terminal primitive** (`formal/tla/README.md` "Scope & limits"). Modelling
  failure propagation would require adding a failure step status + a workflow-level FAILED status + the `ctx.fail`
  terminal transition, a whole new state concept outside that scope, bloating a model kept tiny so TLC finishes in
  seconds. INV-16 is an implementation/DST property (it is about the engine's failure-propagation / workflow-FAILED-status
  behaviour across replay, not the leasing protocol), so it is asserted in the simulator only —
  `Invariants.assertFailurePropagation` (per `workflowId`, scoped to the failing id prefix), exercised by the
  deterministic `FailurePropagationScenario` (`Inv16FailurePropagationTest`, seed 0). Like INV-9 it is **not** added to
  `DstSimulation`'s per-step always-on set: folding a deliberately-FAILING instance into the always-on fuzz would
  complicate the F-0 effect-count documentation (its failing-step effect runs `maxRetries+1` times by design, which the
  fuzz's F-0 detector would otherwise read as duplication) and the liveness expectations, so INV-16 is pinned by the
  deterministic scenario/test instead — the same trade-off INV-9 documents.

---

### INV-17: Status hook fires at most once per status
- **Name:** `StatusHookFiresOncePerStatus`
- **Kind:** Safety
- **Plain English:** A registered workflow status-change hook (a `@WorkflowStatusChangedHandler` / the test
  `.registerWorkflowStatusChangeListener(status, listener)` registration) fires **at most once** for a given
  `(workflowId, status)` across the instance's whole lifetime — and is **NOT re-fired on a crash + replay** that re-runs
  the body and re-drives the engine's event evolution. A status hook is a user side effect (it runs application code on a
  status transition), so — exactly like a step's `execute` action under INV-6 (`EffectAtMostOnce`/F-0) — replaying the
  committed status events must not re-invoke it. This is the **lifecycle-hook analogue of INV-6/F-0**. The STARTED hook
  additionally fires **exactly once** (the reliable at-least-once facet — the start path drains the per-instance task
  queue before the body proceeds, so the STARTED evolution always runs).
- **Formal-ish:** for each hook `workflowId` (those of the hook workflow, scoped to the `hook-` prefix) and each
  registered `status`: let `fires(w, status)` be the number of times the registered listener was invoked for
  `(w, status)` across the instance's whole lifetime (including across crash + replay). Then `fires(w, status) ≤ 1`
  (at-most-once / no-re-fire — the property this invariant establishes), and `fires(w, STARTED) = 1` (the reliable
  at-least-once facet). Asserted on **content** (a fire COUNT per `(workflowId, status)`, kept across `crashAndRecover()`
  exactly like a real external hook effect the engine cannot roll back), **not** the global-append order between an
  instance's own events (F-2-robust). Taken **per `(workflowId, status)`** (never pooled): another instance's hook fire
  is irrelevant; one status's hook firing twice for this instance — a replay re-invoking the listener — is the break.
- **Engine mapping:** A registered status-change listener fires from `EventSourcedWorkflowState.setStatus(...)` →
  `listenerSupport.notify(status)` (`EventSourcedWorkflowState.java:349`/`:413-421`), reached from `evolve(...)` whenever
  a workflow-status event is applied (`:251-281`). The terminal guard `if (workflowStatus().isTerminal()) return;`
  (`:253`) prevents a *second* transition firing within one state object's lifetime. `SimpleWorkflowExecution` always
  constructs the state **with** the listeners (`:127-132`), and on recovery the committed status events flow through
  `onEvent` → `evolve` in replay mode (`:362-363`); empirically the recovered engine does **not** re-invoke the listeners
  while re-evolving the already-committed STARTED/COMPLETED status events (no-re-fire holds). **A distinct facet the
  invariant surfaced (finding F-4, now FIXED):** the terminal **COMPLETED** hook was fired at-most-once but **not
  reliably at-least-once** — on the happy completion path `executeWorkflow` sent the COMPLETED event and only awaited its
  *commit* (`completedWorkflow(...).get(5s)`, `:218-222`) — it did **not** `awaitStateChange` on the terminal status
  before the body returned, unlike the `ctx.fail`/`ctx.cancel`/timeout paths which do (`handleWorkflowException`,
  `:249`/`:266`/`:283`). The body then returned → `finishWorkflow` ran `taskQueue.clear()` (`:336`), so if the live
  evolution of the committed COMPLETED workflow-status event had not yet been consumed off the per-instance queue, its
  `setStatus(COMPLETED)` → listener `notify` was cleared and **never ran** — the COMPLETED hook was silently dropped.
  **F-4 is now fixed:** the happy completion path now also `awaitStateChange(s -> s.workflowStatus().isTerminal())`s on
  the terminal status before `finishWorkflow` (symmetric with the awaited fail/cancel/timeout paths), so the COMPLETED
  evolution and its hook run before the queue is cleared and the COMPLETED hook fires reliably exactly once. The STARTED
  hook fires reliably for the same reason — the start path drains the queue (`publishStartWorkflow` → `awaitStateChange`,
  `:182-187`).
- **Currently holds?** **Yes — both facets now hold.** **The at-most-once / no-re-fire facet (the F-0 analogue this
  invariant establishes): HOLDS.** No registered status's hook is observed to fire more than once for an instance,
  including across (repeated) crash + replay — the recovered engine does not re-invoke listeners while it re-evolves the
  already-committed status events. The STARTED hook fires reliably exactly once. **The terminal COMPLETED at-least-once
  facet now HOLDS too (F-4 FIXED).** Previously the terminal **COMPLETED** hook was fired at-most-once but **not
  guaranteed at-least-once** — on the happy completion path it was non-deterministically **dropped** (fired zero times)
  because `finishWorkflow`'s `taskQueue.clear()` could race ahead of the live evolution of the committed COMPLETED
  workflow-status event, asymmetric with the `ctx.fail`/`ctx.cancel`/timeout terminal paths (which `awaitStateChange` on
  the terminal status before the body returns) and with STARTED (which awaits its evolution). **The engine's happy
  completion path now `awaitStateChange(s -> s.workflowStatus().isTerminal())`s on the terminal status before
  `finishWorkflow` (symmetric with the awaited fail/cancel/timeout paths and STARTED)**, so the COMPLETED evolution and
  its hook run before the queue is cleared and the COMPLETED hook fires reliably exactly once. It is now **enforced** by
  `Invariants.assertCompletedHookFiredExactlyOnce` (alongside `Invariants.assertStartedHookFiredExactlyOnce`); the DST
  scenario asserts the fixed COMPLETED count of exactly one. `Invariants.documentTerminalHookMayBeDropped` is retained as
  the re-fire guard / drop detector (post-fix it always returns false). F-4 is formalized as FIXED in
  `formal/POC-TLA-DST.adoc`.
- **Checked by:** DST (deterministic scenario only; **not** in the per-step fuzz set — Phase-3 scenario-pinned, exactly
  like INV-9 `TimeoutsFire` and INV-16 `FailurePropagation`). **Scope decision (TLA+):** kept DST-only, exactly like
  INV-7..16. Lifecycle hooks are outside the leasing/crash-recovery TLA+ model scope: the Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is scoped to leasing + crash-recovery and has **no notion of workflow status-change
  listeners, a `@WorkflowStatusChangedHandler` registration, or the engine's `setStatus`→`notify` callback path**
  (`formal/tla/README.md` "Scope & limits"). Modelling hook firing would require adding a listener-registry + a callback
  side-effect counter outside that scope, bloating a model kept tiny so TLC finishes in seconds. INV-17 is an
  implementation/DST property (it is about the engine's lifecycle-hook firing across replay, not the leasing protocol),
  so it is asserted in the simulator only — `Invariants.assertStatusHookFiresOncePerStatus` +
  `Invariants.assertStartedHookFiredExactlyOnce` + `Invariants.assertCompletedHookFiredExactlyOnce` (the enforced facets,
  the last now enforcing the COMPLETED at-least-once direction — F-4 FIXED) and `Invariants.documentTerminalHookMayBeDropped`
  (retained as the re-fire guard / drop detector; post-fix always false), per `(workflowId, status)`, scoped to the
  `hook-` id prefix, exercised by the deterministic `StatusHookFiresOncePerStatusScenario`
  (`Inv17StatusHookFiresOncePerStatusTest`, seed 0). Like INV-9/16 it is **not** added to `DstSimulation`'s per-step
  always-on set: a hook-counting always-on fuzz instance would complicate the F-0 effect-count documentation and the
  liveness expectations, so INV-17 is pinned by the deterministic scenario/test instead — the same trade-off INV-9/16
  document. The standalone `HookOnCompletion.tla` model corroborates the fix at design level: `MC_hook.cfg`
  (`AWAIT_TERMINAL_STATE=FALSE`) **violates** `StatusHookFiresOncePerStatus` with the clear-before-notify happy-path
  counterexample (the old behaviour), and `MC_hook_fixed.cfg` (`AWAIT_TERMINAL_STATE=TRUE`) passes with `No error`.

---

### INV-18: Drift guard pauses cleanly
- **Name:** `DriftGuardPausesCleanly`
- **Kind:** Safety
- **Plain English:** When replay drift is detected — `guardAgainstReplayDrift` throws `WorkflowReplayDriftException`
  because new code runs past a step the recorded state already has terminal, WITHOUT a `ctx.migrateVersion` — the engine
  PAUSES the instance **NON-TERMINALLY** and **CLEANLY**: it appends NO terminal workflow-status event and NO
  spurious/corrupt step event for that instance (in particular no event for the drifted step the new code was about to
  publish), and the instance's previously-committed history is left intact — a superset-preserving, recoverable state.
  The drift-paused instance is **exactly the documented INV-5 (`EventuallyTerminates`) non-termination carve-out** (INV-5
  "Currently holds?": the liveness property "**deliberately does NOT hold** for the drift-paused state (by design,
  awaiting redeploy)"). This is the safety twin of that liveness carve-out: INV-5 says a drift-paused instance is
  *allowed* to stall; INV-18 says that when it does, the engine's pause is *clean* (it neither drives the instance
  terminal nor corrupts its committed history).
- **Formal-ish:** for each drifting `workflowId` (those of the drifting workflow, scoped to the `drift-` prefix) whose
  pre-drift committed snapshot `pre(w)` is taken just before the divergent replay, after the divergent replay produces
  `post(w)`: `∄ e ∈ post(w) : e.workflowStatus ∈ {COMPLETED, FAILED, CANCELLED, TIMED_OUT}` (the instance recorded NO
  terminal workflow status — it is PAUSED, not failed/cancelled/completed; a terminal status here = the engine drove a
  drifted instance terminal, the break), and `∄ e ∈ post(w) : e.step = driftedStep(w)` (NO event for the drifted step the
  new code was about to publish — the guard fires *before* the first publish, so the drifted step must never appear), and
  `multiset(pre(w)) ⊆ multiset(post(w))` (committed history intact — every event the instance had before the drifted
  replay is still present, a per-instance **multiset superset/⊇** check). Asserted on **content** (presence/absence/counts
  per `workflowId`), **not** the global-append order between this instance's own events — F-2-robust (the engine publishes
  durably-async, §11; this never asserts the order of an instance's own events). Taken **per `workflowId`** (never
  pooled): another instance's event is irrelevant; this instance being driven terminal, gaining a spurious drifted-step
  event, or losing committed history is the break. The history-intact facet shares the durability contract of INV-3
  (`CommittedHistorySurvivesCrash`).
- **Engine mapping:** The drift guard is `WorkflowExecution.guardAgainstReplayDrift(aboutToExecute)`
  (`WorkflowExecution.java:282-287`): it throws `WorkflowReplayDriftException` iff `hasUnreferencedTerminalStep()` —
  there exists a step that is terminal in the event-sourced `state()` but not in the runtime "book" `referencedStepNames`
  (`unreferencedTerminalSteps()`, `:254-260`). The runtime book is reset at the top of every body run
  (`SimpleWorkflowExecution.java:204`, `referencedStepNames.clear()`) and a step is added to it when its primitive is
  invoked (`recordStepReference(stepName)`, called first thing in `ExecuteDelegate.java:113`/`WaitForDelegate`/
  `PayloadDelegate`/`VersionDelegate`/`TerminateDelegate`); the guard is then called **before the first publish** of a
  step *not* already in state (`ExecuteDelegate.java:117-118`: `if (!state().containsStep(stepName)) {
  guardAgainstReplayDrift(stepName); ... }`). So when "new code" runs past a recorded-terminal step it does not reference
  and reaches a *new* publishing primitive, that recorded-terminal step is an unreferenced orphan → the guard throws.
  `SimpleWorkflowExecution.handleWorkflowException`'s `case WorkflowReplayDriftException` branch (`:289-297`) then logs a
  warning and **intentionally does NOT publish failedWorkflow/cancelledWorkflow events**, leaving the workflow in its
  current (non-terminal) state — "the next replay will try again. If the developer reverts the offending code or adds
  `ctx.migrateVersion(...)`, the replay will run cleanly and the workflow continues normally." No event for the drifted
  step is appended (the guard threw before its first publish), and nothing in this path mutates the committed log, so the
  previously-committed history is left intact (the cached-result replay path for the already-recorded steps emits
  nothing, `ExecuteDelegate.java:117`). This is the documented behaviour of `WorkflowReplayDriftException` itself (its
  Javadoc: "Caught and handled non-terminally … *no* terminal workflow event is published, so the workflow stays at its
  current state"). See axon-flow-workflow skill §3.6 (drift guard fires before every state-publishing primitive) and
  ADR-005's drift-pause.
- **Currently holds?** Yes (honest, **empirically confirmed by live drift induction** — not merely documented). Drift was
  induced LIVE in the DST harness: a `DriftWorkflow` instance recorded under its v1 body (`reserveInventory` +
  `chargePayment` recorded-terminal, then a never-arriving wait keeping it LIVE) was crash + recovered under a
  structurally-divergent v2 body that skips the recorded-terminal `chargePayment` and reaches a NEW `repackage` step,
  WITHOUT `ctx.migrateVersion`. The recovered engine replayed the committed log, re-created the live instance, re-ran the
  v2 body, and `guardAgainstReplayDrift("repackage")` threw `WorkflowReplayDriftException` (confirmed by the logged
  warning: *"Workflow drift-A cannot proceed at "repackage" because history contains terminal steps the current code does
  not reference: [chargePayment]"*). The instance then stayed PAUSED: no terminal workflow status, no `repackage` step
  event, and its committed history was byte-for-byte identical to the pre-drift snapshot (5 events: STARTED,
  `reserveInventory` STARTED/COMPLETED, `chargePayment` STARTED/COMPLETED). The `repackage` side effect never ran (count
  0). So the drift-pause is exactly the clean, non-terminal, history-preserving carve-out INV-18 asserts — and exactly
  the INV-5 carve-out. **No anomaly observed** (a terminal event or corrupted/truncated history on drift would be a
  high-value finding — none occurred). A genuine break (the engine driving a drifted instance terminal, a spurious
  drifted-step event, or a lost committed event) throws `InvariantViolation`, to be triaged per the POC rules.
- **Checked by:** DST (deterministic scenario only; **not** in the per-step fuzz set — Phase-3 scenario-pinned, exactly
  like INV-9 `TimeoutsFire`, INV-16 `FailurePropagation` and INV-17 `StatusHookFiresOncePerStatus`). **Scope decision
  (TLA+):** kept DST-only, exactly like INV-7..17. The drift guard is outside the leasing/crash-recovery TLA+ model
  scope: the Phase-2 TLA+ model (`WorkflowLeaseRecovery.tla`) is scoped to leasing + crash-recovery and has **no notion of
  the runtime step-reference "book" (`referencedStepNames`), the event-sourced-vs-runtime book comparison
  (`unreferencedTerminalSteps`/`guardAgainstReplayDrift`), workflow versions, or the `ctx.migrateVersion` primitive**
  (`formal/tla/README.md` "Scope & limits"). Modelling drift detection would require adding a per-invocation reference set
  + a divergent-code transition + the migrateVersion fork outside that scope, bloating a model kept tiny so TLC finishes
  in seconds. INV-18 is an implementation/DST property (it is about the engine's drift-pause behaviour across a divergent
  replay, not the leasing protocol), so it is asserted in the simulator only — `Invariants.assertDriftGuardPausesCleanly`
  (per `workflowId`, scoped to the `drift-` id prefix, content-based / F-2-robust), exercised by the deterministic
  `DriftGuardPausesCleanlyScenario` (`Inv18DriftGuardPausesCleanlyTest`, seed 0) which induces drift LIVE and asserts the
  clean pause, plus non-trivial assertion pins (a drift-paused instance that recorded a terminal status throws; a spurious
  drifted-step event throws; a truncated/corrupted history throws; the good case passes). Like INV-9/16/17 it is **not**
  added to `DstSimulation`'s per-step always-on set: a drift-inducing always-on instance would deliberately stall
  non-terminally (the INV-5 carve-out), which the fuzz's liveness horizon check would otherwise read as a hang, so INV-18
  is pinned by the deterministic scenario/test instead — the same trade-off INV-9/16/17 document.

---

### INV-19: Payload reducer semantics
- **Name:** `PayloadReducerSemantics`
- **Kind:** Safety
- **Plain English:** Each payload reducer produces its **documented merge** into the workflow payload — and that result
  is stable across crash/replay. **Extends INV-13** (`NoLostPayloadWrites`): INV-13 covered combine + local_only on the
  RESULT side for no-lost-write; INV-19 covers each reducer **type**'s documented MERGE semantics, on **both** the
  parameter and result sides. Concretely, for a step's recorded payload contribution: **`GlobalOnlyPayloadReducer`** (the
  default `resultPayloadReducer`) DISCARDS the step's result — the payload is unchanged (the result's keys do NOT appear
  from this step, unless written elsewhere); **`CombineGlobalAndLocalPayloadReducer`** MERGES the step's result map
  key-by-key into the running payload (each `k=v` added/overwritten); **`LocalOnlyPayloadReducer`** (used by
  `setPayload`/`modifyPayload`) REPLACES the payload wholesale by the step's map (prior keys not in the replacement are
  dropped); and **`parameterPayloadReducer`** (default `LocalOnly`) governs what the step's action SEES as its input
  payload (the step's local view) vs the global — the documented parameter-side default. The engine's reconstructed final
  payload for the instance must reflect exactly these documented behaviours: a `global_only` step's result keys are
  absent, a `combine` step's keys are present, a `local_only` replace drops prior keys not in the replacement. The
  invariant additionally pins the reducer **edge cases** (all per `workflowId`, content-based, F-2-robust): **(a)** a
  `combine` step whose result map carries a **`null` value** — per the documented combine fold
  (`new HashMap<>(global); putAll(local)`, where `HashMap.putAll` inserts a null value) the key is PRESENT with value
  `null` (combine OVERWRITES with null; it does NOT skip it nor keep a prior global value) — **but the engine does NOT
  support a null payload value end-to-end: candidate finding F-6** (`EventSourcedWorkflowState.payload()` does
  `Map.copyOf(payload)`, which rejects null values, so the very next payload read throws and the instance is wedged
  non-terminally — characterized + flagged, not patched; see POC-TLA-DST.adoc F-6); **(b)** the **parameter-view and
  result-write reducers are independent knobs** — a step whose `parameterPayloadReducer` gives it a combined INPUT view
  but whose `resultPayloadReducer` is `global_only` writes nothing back (its result is discarded); **(c)**
  **last-writer-wins on the same key** — when two steps write the same payload key under `combine`, the later (in this
  instance's own step sequence — the deterministic order, never the global-append order) write wins in the reconstructed
  payload; **(d)** **missing keys under combine** — a `combine` step's result map that OMITS a key the running payload
  already holds keeps that key's prior (global) value (only the present keys are merged); and **(e)** the whole fold is
  **crash/replay-stable** — replaying the same history re-applies the same reducers to the same recorded results,
  rebuilding the identical payload (no double-merge, no lost write).
- **Formal-ish:** for each reducer `workflowId` (those of the reducer workflow, scoped to the `reducer-` prefix), define
  the EXPECTED payload **(A)** as the fold of the instance's committed events in append order, applying each
  payload-bearing event's named reducer exactly as `EventSourcedWorkflowState.evolvePayload` does (`:307-335`):
  `combine_local_and_global` merges each `k=v` of the event map into the payload, `local_only` replaces the whole payload
  with the event map, `global_only`/unknown leave the payload unchanged (the result discarded). Define the ACTUAL payload
  **(B)** as the engine's own reconstructed payload for the instance (the workflow-history read-model's
  `state().payload()`, evolved by the `WorkflowHistoryProjector` — an evolution path independent of the fold in (A)).
  Then INV-19 requires `(A) == (B)` key-for-key: the engine applied each reducer exactly as documented. A divergence —
  a `global_only` result key WRONGLY present in (B), a `combine` key MISSING from (B), a `local_only` replace that did
  NOT drop a prior key (a stale key present in (B)), or any value mismatch — is the break. The replay-stability facet:
  both (A) and (B) are pure functions of `history(w)`, so `replay(history(w))` rebuilds the identical payload — enforced
  jointly with INV-4 (`DeterministicReplay`), and (B) is re-derived by the projector across the crash/replay, so a replay
  that mis-applied a reducer would show. Asserted on **content** (the per-`workflowId` reconstructed payload), **not** the
  global-append order between this instance's own events — F-2-robust, exactly like INV-13. Taken **per `workflowId`**
  (never pooled): the single global log interleaves independent instances non-deterministically (the F-2 surface, §11),
  so another instance's payload is irrelevant; one instance's reconstructed payload diverging from its own documented
  reducer fold is the break.
- **Engine mapping:** The three reducers live in `runtime`: `GlobalOnlyPayloadReducer.apply(global, local)` returns
  `global` (the result discarded; `NAME = "global_only"`), `CombineGlobalAndLocalPayloadReducer.apply` returns
  `new HashMap<>(global); putAll(local)` (a key-by-key merge; `NAME = "combine_local_and_global"`),
  `LocalOnlyPayloadReducer.apply` returns `local` (a whole-payload replace; `NAME = "local_only"`). A step's COMPLETED
  event carries the step's `resultPayloadReducer.name()` on the `modifyPayload` metadata key
  (`ExecuteDelegate.java:174`, `completed(stepName, r, resultPayloadReducer.name(), …)`); the default `execute`
  `resultPayloadReducer` is `GlobalOnlyPayloadReducer` so a plain `execute` result is discarded (axon-flow-workflow skill
  §5), while `setPayload`/`modifyPayload` publish under `local_only` (`PayloadDelegate.java:87-91`). The
  `parameterPayloadReducer` (default `LocalOnlyPayloadReducer`) prepares the step's action input —
  `ExecuteDelegate.java:146` `var payload = parameterPayloadReducer.apply(workflowContext.workflowPayload(), local)` — so
  `local_only` (default) passes only the step's local input while `combine_local_and_global` gives the action `global ∪
  local` (it sees the workflow payload) — these are **independent knobs** (edge (b)): a combined parameter view does not
  imply a combined result write, e.g. `parameterPayloadReducer(combine)` + `resultPayloadReducer(global_only)` sees the
  global payload yet writes nothing back. `evolvePayload` (`EventSourcedWorkflowState.java:307-335`) looks the named
  reducer up in the `PayloadReducerRegistry` and applies it to `(this.payload, event.payload)`, rebuilding the payload
  deterministically from the committed log; because `combine`'s `putAll` is applied in the committed-log append order,
  two same-key combine writes resolve **last-writer-wins** per the instance's step sequence (edge (c)) and a combine that
  omits a key keeps the prior global value (edge (d)). A replay re-runs the deterministic body and a present terminal
  step yields a cached result and emits nothing (`ExecuteDelegate.java:117`), so each reducer is re-applied identically
  (edge (e)). **Edge (a) `null` value:** `combine`'s `new HashMap<>(global); putAll(local)` accepts a null map value, but
  `EventSourcedWorkflowState.payload()` (`:176`) returns `Map.copyOf(payload)` and `Map.copyOf` rejects null values —
  the next payload read (at workflow completion, `SimpleWorkflowExecution.executeWorkflow:219` →
  `WorkflowContextDelegation.workflowPayload`) throws `NullPointerException`, which lands in
  `handleWorkflowException`'s `default` branch (`SimpleWorkflowExecution.java:311-323`) that deliberately records **no**
  terminal status, wedging the instance — **candidate finding F-6** (POC-TLA-DST.adoc), characterized not patched.
- **Currently holds?** Yes (honest assessment) — the engine applies the named reducer in `evolvePayload` for every
  payload-bearing committed event exactly as each reducer's `apply` documents (global_only discards, combine merges,
  local_only replaces), and a deterministic body + the cached-result replay path mean a replay rebuilds the identical
  payload. The DST harness asserts this after every step (including across the crash/restart/reorder faults, against a
  `ReducerWorkflow` whose steps deterministically exercise all three reducers AND the reducer edge cases — a combine
  seed, a `local_only` `modifyPayload` replace dropping the seed, a combine that must appear, a `global_only` `execute`
  whose result must be discarded, a `parameterPayloadReducer(combine)` step that must SEE the combined global payload, an
  interplay step whose combine parameter view but `global_only` result write means its result is discarded (edge (b)),
  and a last-writer-wins same-key combine pair (edge (c); edge (d) missing-key-under-combine rides the same body as every
  combine omits keys the payload already holds, which survive)) by cross-checking the engine's own reconstructed payload
  against the documented reducer fold of the committed log (two independent reconstructions), and the dedicated scenario
  asserts a fresh start's final payload demonstrates each reducer's documented outcome (combine key present, global_only
  result key absent, local_only-dropped prior key absent, parameter-side combine view `true`, interplay key absent,
  last-writer-wins key carrying the later value) and a crash + replay rebuilds the identical payload (edge (e)). No
  counterexample observed across the fuzz campaign for the supported reducer/value space. **One edge IS a candidate
  finding: edge (a) a `null` value under combine — F-6** (`EventSourcedWorkflowState.payload()`'s `Map.copyOf` rejects
  the null and the completion-path read throws an NPE that `handleWorkflowException`'s `default` branch does NOT turn
  into a terminal status, wedging the instance non-terminally) — characterized by the scenario's
  `runNullEdge`/`nullValueUnderCombine_wedgesTheInstance_candidateFindingF6` pin and flagged in POC-TLA-DST.adoc, **not
  patched** (test/docs-only rule). (A genuine break in the supported space — the engine's reconstructed payload diverging
  from the documented reducer fold: a global_only result that appears, a combine key that is missing, a local_only
  replace that does not drop a prior key, an earlier same-key writer that wins — would be a high-value finding, the
  engine mis-applying a documented reducer, to be triaged per the POC rules, not silently tolerated.)
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7..18. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and has **no notion of a workflow
  payload, payload reducers, the three reducer behaviours (merge / replace / discard), or the parameter-vs-result reducer
  distinction** — its `log` is a sequence of `<<step, status>>` step records with no payload map (`formal/tla/README.md`
  "Scope & limits"), exactly as noted for INV-13. Modelling reducer semantics would require adding a payload map + the
  three reducer behaviours + the parameter/result-side distinction, a whole new state concept outside that scope,
  bloating a model kept tiny so TLC finishes in seconds. INV-19 is an implementation/DST property (it is about the
  engine's reducer-application behaviour across replay, not the leasing protocol), so it is asserted in the simulator
  only — `Invariants.assertPayloadReducerSemantics` (per `workflowId`, scoped to the `reducer-` id prefix, content-based /
  F-2-robust), exercised by `DstSimulation` (every step + a horizon pin, against a `ReducerWorkflow` whose body exercises
  the three reducers plus edges (b)/(c)/(d), driven alongside the order workhorse through the full fault set, folded into
  the fuzz exactly like INV-13) and the deterministic `PayloadReducerSemanticsScenario` (`Inv19PayloadReducerSemanticsTest`,
  seed 0). The edges: (b)/(c)/(d) ride the fuzz `ReducerWorkflow` (serialization-safe, content-based, so no append-order
  flake); edges (a) (the `null`-value combine → **F-6**) and (e) (replay-stability with a `null` value) are
  **scenario-pinned** (`PayloadReducerSemanticsScenario.runNullEdge` + `ReducerWorkflow.nullEdge`, a distinct
  `ReducerNullEdgeRequestedEvent` start, kept OUT of the always-on fuzz so a serialization quirk on a null map value
  cannot flake the 1000-seed sweep) and the null case is characterized as F-6 rather than asserted to pass. The same
  reducer-edge work **generalized F-6 (S-4):** a `modifyPayload` modifier lambda that throws a plain `RuntimeException`
  between primitives wedges the instance via the SAME `handleWorkflowException` `default`-branch sink (not just the
  null-payload completion-read NPE) — also **scenario-pinned** (`PayloadReducerSemanticsScenario.runThrowingModifierEdge`
  + `ReducerWorkflow.throwingModifier`, a distinct `ReducerThrowingModifierRequestedEvent` start, registered scenario-only
  via `EngineInstance.reducerThrowingModifierWorkflow`, kept OUT of the always-on fuzz because a deliberately-wedging body
  leaves an instance non-terminal which the liveness-horizon check would correctly read as a hang) and characterized as
  F-6/S-4 rather than asserted to pass. See the INV-5 `EventuallyTerminates` carve-out note + the INV-16
  `FailurePropagation` note above, and POC-TLA-DST.adoc finding **F-6** (generalized by **S-4**).
- **F-2-robust against the post-`RESTART` global-append re-interleaving (the expected fold (A) is built in logical step
  order):** the cross-check requires the harness's expected fold **(A)** to match the engine's reconstructed payload
  **(B)**. (B) is correct — the engine applies each step's payload in the instance's **deterministic logical order**.
  But (A) was originally folded in the **global-append order** of the single shared event store, and that order is **not**
  stable across a `RESTART`: a crash mid-instance and the recovery re-drive can land a `local_only` `modifyPayload`
  COMPLETED event in the global log **before** an earlier `combine` step's COMPLETED event whose append was delayed by
  the crash (observed at seeds 150/317 under the heavier fuzz load: `localOnlyReplace` COMPLETED appended at index 44,
  but the earlier `seedWrite` (combine) COMPLETED at index 65). An append-order fold then applies the whole-payload
  `local_only` replace first and the earlier `combine` second, **resurrecting `seedKey`** that the replace was supposed
  to drop — so (A) carried `seedKey` while the correct (B) did not. This is the **F-2 intra-instance append-order
  surface** the invariant's contract explicitly excludes ("asserted on **content**, **not** the global-append order
  between this instance's own events" — §11) leaking into the *expected*-side reconstruction; the seeds pass
  deterministically in isolation (the lighter reproduce never produces the re-interleaving), so it is a load-flake, the
  reducer semantics + execution unaffected. **Fix (test/harness-only):** INV-19's (A) is now reconstructed by
  `Invariants` in the instance's **deterministic logical step order** (each step ordered by the **minimum committed-log
  index among its events** — the `STARTED` for an `execute`, the directly-published `COMPLETED` for a `modifyPayload`
  that has no `STARTED` — which the body issues in declaration order even when the crash re-orders the later
  `COMPLETED`s, then the payload-bearing `COMPLETED` contributions folded in that order), matching the engine's
  evolution. This changes only the **fold ORDER of (A)**, never what it asserts: a PERSISTENT reducer mis-application (a
  `global_only` result that leaked in, a `combine` key missing, a `local_only` replace that kept a prior key, a wrong
  last-writer) still makes (A) diverge from (B) and trips — and the horizon pin
  (`DstSimulation.assertReducerInstanceDemonstratedEveryReducer`) independently re-asserts the complete final payload.
  The `PayloadReducerSemanticsScenario`/`Inv19PayloadReducerSemanticsTest` pins are unaffected (their hand-built logs
  have append order == logical order, so the reordered fold is identical).

---

### INV-20: Versioning edges
- **Name:** `VersioningEdges`
- **Kind:** Safety
- **Plain English:** The engine's versioning machinery is sound at the **edges** INV-11 (`VersionRoutingSound`) and
  INV-12 (`MigrateVersionContract`) do not cover. For a workflow registered at **three** coexisting versions whose body
  performs multiple in-body `ctx.migrateVersion` migrations under distinct `changeId`s and attempts a downgrade, across
  crashes/replays: **(1) a downgrade migration is rejected and never recorded** — `ctx.migrateVersion(changeId, vLower)`
  where `vLower` is not strictly greater than the instance's current recorded version throws
  `IllegalArgumentException` and writes no marker (§3.7 contract); **(2) multiple distinct `changeId`s each record at
  most once and monotonic non-decreasing** (extends INV-12 across `changeId`s); **(3) deeper multi-version routing**
  (extends INV-11 with depth) — a fresh start spawns at the **highest** registered version (the `STARTED` event carries
  it), no committed event is stamped below the started version, and an instance recorded (post-migration) at a version
  no longer registered, recovered under a reduced registry, routes via the 4/5-pass lookup to the **closest registered
  sibling** ≤ its recorded state — **never 0** (stranded), **never 2** (two definitions handling one instance).
- **Formal-ish:** for each versioning-edges `workflowId` (those of the edges workflow, scoped to the `vedge-` prefix):
  let `markers(w)` = the instance's committed migration-marker events (`MetadataUtils.isMigrationStep`), each carrying a
  `versionChangeId` + `version`. Then **(downgrade rejected)** `∀ m ∈ markers(w) : m.versionChangeId ≠ downgradeChangeId`
  (the downgrade `changeId` is never recorded — the throw happens before any append), and no `m.version` is `<` the
  instance's started version (a recorded stamp downgrade); **(at-most-once + monotonic)** `∀ changeId c :
  Cardinality({m ∈ markers(w) : m.versionChangeId = c}) ≤ 1`, and scanning `markers(w)` in committed (append) order the
  recorded `version` values are non-decreasing (`∀ i<j : version(m_i) ≤ version(m_j)`), each a valid semver; **(routing
  depth)** the `STARTED` event's `MessageType.version()` equals `max(registeredVersions)` for a fresh spawn, and every
  committed event's version is `≥` the started version (the resolved version only moves forward). Unlike INV-11 (a
  **non-migrating** instance carries exactly one version), this instance **migrates**, so its events legitimately carry
  the started version plus the forward-bumped versions; "never 0" is the presence check (an issued start produced an
  instance carrying a resolvable version), and "never 2" (two definitions concurrently handling one instance) is
  observed by the instance recovered under the reduced registry still completing cleanly under the one closest sibling
  the lookup routed to (it found a runnable body, drift-free). The replay-stability facet: `markers(w)` and the version
  stamps are pure functions of `history(w)`, so `replay(history(w))` re-reaches each migration as a no-op and resolves
  the same versions — enforced jointly with INV-4 (`DeterministicReplay`). Taken **per `workflowId`** (never pooled): the
  single global log interleaves independent instances non-deterministically (the F-2 surface, §11), so another instance's
  marker/version landing here is irrelevant; one instance recording a downgrade, re-applying a marker, recording a
  version below an earlier one, spawning below the highest, or being stranded/double-handled on recovery is the break.
- **Engine mapping:** A downgrade is rejected in `VersionDelegate.version` (`VersionDelegate.java:117-122`): when the
  requested version is not strictly greater than `state.workflowVersion()`, it throws `IllegalArgumentException`
  **before** appending any task (the `appendTask` for the marker is at `:140`), so no marker is recorded. Multiple
  distinct `changeId`s are each guarded by `state.hasMigrateVersionStep(changeId)` (`:103`) + `evolve`'s
  `versions.putIfAbsent(changeId, version)` first-writer-wins (`EventSourcedWorkflowState.java:293`) and the
  strictly-greater `workflowDefinitionVersion` bump (`:295-297`), so each records once and the recorded version moves
  only forward. A fresh start spawns at the highest registered version via
  `WorkflowConfigurationRegistry.getHighestVersionConfigurations` (`WorkflowConfigurationRegistry.java:74-86`) through
  `WorkflowEngine.checkAndCreateNewWorkflow`, stamping the `STARTED` event with it (`EventMessageUtils.startedWorkflow`).
  In-flight/recovery routing for an instance whose recorded state version is no longer registered exactly runs the
  4/5-pass lookup `resolveDefinitionForReplay` (`WorkflowConfigurationRegistry.java:249-305`): pass 1 exact-spawn-config,
  pass 2 exact-sibling, **pass 3 closest-sibling ≤ recorded state** (`findClosestRegisteredVersion` →
  `Version.closestNotGreaterThan`), pass 4 closest-higher within the major, pass 5 no-match-fallback. Cross-version
  collisions augment the id with `#<version>` (`WorkflowSpawnRouting.java:77`). The DSL contract (axon-flow-workflow §3.7;
  `BaseWorkflowContext.migrateVersion` Javadoc): call at most once per `changeId`, never in loops/combinators; downgrades
  throw.
- **Currently holds?** Yes (honest assessment) — `VersionDelegate` rejects the downgrade with `IllegalArgumentException`
  before recording (the marker for the downgrade `changeId` is never written), the per-`changeId` first-writer-wins +
  strictly-greater bump keep multiple recorded versions written-once and monotonic, the fresh spawn picks the highest
  registered version, and the 4/5-pass lookup routes a recovered instance recorded at a no-longer-registered version to
  the closest registered sibling (a runnable body, not 0/not 2). The DST harness asserts this after every step
  (including across the crash/restart/reorder faults, against a `VersioningEdgesWorkflow` registered at three versions
  whose body does two forward bumps under distinct `changeId`s + a rejected downgrade, driven alongside the order
  workhorse) and the dedicated scenario asserts a fresh start spawns at the highest version, rejects the downgrade
  (recording an observable `downgradeRejected` step, no downgrade marker), records the two forward markers monotonic,
  resolves the same record after a crash + replay, and — driving a second instance to the suspension point recorded
  post-migration then recovering it under a registry whose highest version was dropped — routes it to the closest
  registered sibling and completes it (the routing landed on a runnable body). No counterexample observed across the
  fuzz campaign. (A genuine break — a recorded downgrade marker, a marker re-applied on replay, a recorded version
  moving backwards, a fresh spawn not at the highest registered version, or a recovered instance stranded/double-handled
  — would be a **high-value versioning finding** to triage per the POC rules, not silently tolerated.)
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7..19. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to a **single** workflow instance over leasing + crash-recovery
  with **no notion of workflow versions, a multi-version registry, the `migrateVersion` primitive, a downgrade-rejection
  rule, or the multi-pass replay-routing lookup** — its `log` is a sequence of `<<step, status>>` records with no version
  stamp and no registry (`formal/tla/README.md` "Scope & limits"), exactly as noted for INV-11/INV-12. Modelling the
  versioning edges would require adding versioned definitions + the highest-version spawn rule + the version map with the
  first-writer-wins/strictly-greater/downgrade-reject apply rules + the 4/5-pass routing lookup, a whole new state
  concept outside that scope, bloating a model kept tiny so TLC finishes in seconds. INV-20 is an implementation/DST
  property (it is about the engine's multi-version spawn/routing + `migrateVersion` recording behaviour at the edges, not
  the leasing protocol), so it is asserted in the simulator only — `Invariants.assertVersioningEdges` (per `workflowId`,
  scoped to the `vedge-` id prefix), exercised by `DstSimulation` (every step + a horizon pin, against a three-version
  `VersioningEdgesWorkflow` driven alongside the order workhorse through the full fault set — folded into the fuzz for
  its downgrade-rejection + multi-`changeId` edges) and the deterministic `VersioningEdgesScenario`
  (`Inv20VersioningEdgesTest`, seed 0 — which additionally drives the deeper closest-sibling routing via
  `crashAndRecoverWith` a reduced registry, the way INV-18 induces drift, since the fixed-registration fuzz loop cannot
  drop a registered version mid-run).

---

### INV-21: Retry timing and exhaustion edges
- **Name:** `RetryTimingAndExhaustionEdges`
- **Kind:** Safety
- **Plain English:** The retry/timeout machinery is sound at the **edges** INV-8 (`RetryBound`) and INV-9 (`TimeoutsFire`)
  did not cover. For a step under a `RetryPolicy` with a `BackoffStrategy`, and a step whose per-attempt `execute` timeout
  is exceeded, across crashes/replays: **(1) backoff-strategy timing** — a step under `BackoffStrategy.fixed`/`linear`/
  `exponential` retries on the schedule reconstructed from the recorded `RETRYING` timestamps (NOT from in-memory state),
  so the schedule survives crash/replay: the recorded attempt timestamps are monotonic in committed time, the gaps are
  constant for `fixed` and non-decreasing for `linear`/`exponential`, and the step records exactly the strategy-dictated
  number of `RETRYING` records and ultimately resolves to a terminal step record; **(2) `retryWhile` predicate** — a
  `RetryPolicy.retryWhile(predicate)` stops retrying when the predicate says so, recording strictly **fewer** attempt
  records than `maxRetries + 1` (the bound INV-8 alone would allow); **(3) `onRetry` not re-run on replay** (the headline
  probe, the F-0 analogue for retry handlers) — an `onRetry` handler side effect fires **exactly once per actual retry
  decision** and is **not** re-invoked when a crash/replay re-reaches the already-recorded `RETRYING` steps; **(4)
  per-attempt `execute` timeout** (the D5 residual INV-9 left to the wait path) — an `execute` step whose action exceeds
  its per-attempt `timeout` reaches a `TIMED_OUT` outcome (recorded), and with `maxRetries(n)` the total budget is
  `(retries + 1) × timeout`; the step reaches a terminal `TIMED_OUT`, never hanging. Builds on INV-8 (the attempt-record
  ceiling) and INV-9 (a configured timeout produces a terminal outcome) by pinning the *strategy/predicate/handler/
  per-attempt-execute-timeout* edges those left open.
- **Formal-ish:** for each retry-edges `workflowId` (scoped to the `retryedge-` prefix), per `(workflowId, stepName)` of a
  step with a configured `RetryStepSpec(maxRetries, expectedRetries, backoffIncreasing)`: let `attempts(w, s)` = the
  step's committed non-terminal records (`STARTED` + `RETRYING`) in append order, `retrying(w, s)` =
  `Cardinality({ a ∈ attempts(w, s) : a.status = RETRYING })`. Then **(within bound)** `retrying(w, s) ≤ maxRetries`
  (so `|attempts| ≤ maxRetries + 1`, the INV-8 ceiling); **(exact count)** `retrying(w, s) = expectedRetries` (≤ maxRetries,
  and strictly `< maxRetries` for the `retryWhile` step); **(schedule monotonic)**
  `∀ i < j : attempts[i].timestamp ≤ attempts[j].timestamp`, and when `backoffIncreasing`,
  `∀ i : gap(i) ≤ gap(i+1)` over successive attempts; **(resolves)** `∃ e ∈ history(w) : e.step = s ∧ e.stepStatus ∈
  TerminalStepStatuses`. For the `onRetry` facet, per `(w, s)`: `onRetryFires(w, s) = retrying(w, s)` (the crash-surviving
  fire count equals the committed `RETRYING` count — a count `>` is the re-fire break, a count `<` is a dropped fire). For
  the per-attempt-execute-timeout step (`retrytimeout-` prefix): its slow `execute` reaches a terminal `TIMED_OUT` step
  record. Taken **per `(workflowId, stepName)`** (never pooled): the single global log interleaves independent instances
  non-deterministically (the F-2 surface, §11); one step recording more retries than the bound, the wrong count, an
  out-of-order or shrinking schedule, never resolving, an `onRetry` re-fire on replay, or a per-attempt-`execute` timeout
  that hangs is the break. Content-based (record counts + recorded timestamps), so robust to the F-2 intra-instance
  global-append-order non-determinism.
- **Engine mapping:** Retries with backoff are driven by `RetryableExecuteDelegate`. On a live failure,
  `handleAttemptFailure` computes the backoff delay from `RetryPolicy.backoffStrategy().delay(attempt)`
  (`RetryableExecuteDelegate.java:136`), evaluates `RetryPolicy.shouldRetry` (= `attempt <= maxRetries &&
  retryPredicate.test(ctx)`, `RetryPolicy.java:119-121` — the `retryWhile` predicate is the second conjunct, stopping
  retries early), and on a retry decision calls `retryPolicy.onRetryHandler().onRetry(retryContext)` (`:140`) **before**
  appending the `RETRYING` event (`:143`) and scheduling the next attempt via
  `scheduler.delayedExecutor(delay)` (`:210`) where `delay` is computed from the recorded attempt timestamp
  (`computeRetryReadyAt(retryPolicy, attempt, …)`, `:176-182`). Across a crash, recovery resumes from the persisted
  `RETRYING` state and re-schedules the next attempt from the recorded `StepRetryInfo.attempt()` + the recorded
  `step.timestamp()` (`:101-107`) **without** re-invoking `onRetry` (the resume path does not call the handler — the
  `RetryHandler` Javadoc states it is "Not called during replay"). So the retry schedule is a pure function of the
  committed `RETRYING` timestamps (survives crash/replay), and `onRetry` fires once per actual retry decision, never on
  replay. The per-attempt `execute` timeout rides `ExecuteDelegate`'s `orTimeout` on
  `actualStartTime.plus(timeout)` vs `Instant.now(clock)` (`ExecuteDelegate.java:134-168`): a negative remaining records
  `TIMED_OUT` immediately (`:160-165`), otherwise the JDK `orTimeout` timer fires it (`:167-180`) — the non-injectable
  residual (ARCHITECTURE.md §12, the adoc's D5), which is why this part is scenario-pinned (driven via the lock-step
  `MutableClock` past the per-attempt window) exactly like INV-9's wait-timeout exemption from the fuzz set.
- **Currently holds?** Yes (honest assessment) — the three backoff strategies retry on the strategy-derived schedule
  reconstructed from the recorded `RETRYING` timestamps and the schedule survives a crash + replay; the `retryWhile`
  predicate stops retrying with strictly fewer attempt records than `maxRetries + 1`; **the headline `onRetry`-not-re-run
  probe HELD** — the crash-surviving `onRetry` fire count was unchanged across a crash + replay (the engine resumes a
  `RETRYING` step from its persisted attempt without re-invoking the handler); and **the per-attempt `execute` timeout
  HELD** — the slow `execute` reached a terminal `TIMED_OUT` and never hung, with the attempt records within
  `maxRetries + 1`. The DST scenario asserts all four edges and a crash + replay. (If `onRetry` were ever found to RE-FIRE
  on replay — fire count exceeding the actual retry count — that is an **F-0-analogue real finding** to triage per the POC
  rules, not silently tolerated; likewise a configured per-attempt `execute` timeout that does not fire, or an attempt
  count exceeding the policy.)
  **Backoff-arithmetic overflow edge — finding F-9 (S-3), liveness-wedge but EXTREME-config / LOW likelihood — FIXED.**
  The backoff *timing* facet (1) above held for the normal `fixed`/`linear`/`exponential` schedules at small attempt
  counts. At LARGE attempt counts the `exponential` factory's arithmetic did NOT: _was:_
  `BackoffStrategy.exponential(base, max)` did `factor = 1L << (attempt - 1); computed = base.multipliedBy(factor);
  return computed > max ? max : computed;` — clamp **after** the multiply. With a large `maxRetries` (e.g. `maxRetries(70)`)
  and a seconds/minutes-scale `base`, `base.multipliedBy(2^(attempt-1))` **overflowed `Duration` and threw
  `ArithmeticException`** at `attempt ≈ 59-63` (before the cap), and the throw — computed on the workflow thread in
  `RetryableExecuteDelegate.handleAttemptFailure` (`:136`) — landed in `handleWorkflowException`'s `default` branch and
  **wedged the instance non-terminally** (the F-6/S-4 sink); for a small base the `attempt == 64` shift went negative
  (`1L << 63 == Long.MIN_VALUE`) and the cap did not clamp the negative `Duration`, and `attempt ≥ 65` masked the shift
  mod 64 to a tiny wrapped factor (a non-monotonic garbage schedule). INV-8's record bound still held (the overflow
  stopped recording early), but the *liveness* did not. _Now (fix):_ `BackoffStrategy.exponential`
  (`BackoffStrategy.java:72-81`) clamps the shift exponent (`1L << min(max(attempt-1,0),62)`) and returns `max` whenever
  `factor > max/base` (or `base ≤ 0`), so the multiply can never overflow, go negative, or wrap; the always-failing
  exponential-backoff step retries on the clamped schedule to exhaustion and goes terminal `FAILED`. See POC-TLA-DST.adoc
  finding **F-9**; DST-confirmed (unit arithmetic + reachable engine terminal, seed 0) by
  `S3BackoffExponentialOverflowTest` (via `BackoffOverflowScenario` + `BackoffOverflowWorkflow`), flipped from
  documenting the wedge to asserting the fixed clamp-and-exhaust property. Real-world likelihood was **LOW** (needs an
  extreme config).
- **Checked by:** DST (deterministic scenario only; **not** in the per-step fuzz set — the per-attempt `execute` timeout
  rides the `orTimeout` residual, Phase-3 D5, exactly like INV-9, and a deliberately-retrying/timing-out always-on fuzz
  instance would complicate the F-0 effect-count documentation + the liveness horizon check, the same reasoning as
  INV-9/16/17/18). **Scope decision (TLA+):** kept DST-only, exactly like INV-7..10/INV-16..18. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and explicitly does **not** model time,
  timeouts, **retries, backoff, retry predicates, or retry handlers** (see `formal/tla/README.md` "Scope & limits"); its
  `log` is a sequence of `<<step, status>>` records with `status ∈ {STARTED, COMPLETED}` and no `RETRYING`/`TIMED_OUT`,
  no backoff timer, no `retryWhile` predicate and no `onRetry` handler callback. Modelling the backoff schedule + the
  retry predicate + the handler-fire side effect + the per-attempt execute-timeout would introduce time/timer/handler
  state concepts outside that scope and bloat a model kept tiny so TLC finishes in seconds. INV-21 is therefore an
  implementation/DST property (it is about the engine's retry/backoff scheduling + `onRetry` replay-safety + per-attempt
  `execute` timeout behaviour at the edges, not the leasing protocol), so it is asserted in the simulator only —
  `Invariants.assertRetryTimingAndExhaustionEdges` (per `(workflowId, stepName)`, scoped to the `retryedge-` id prefix) +
  `Invariants.assertOnRetryFiredOncePerRetry` (the `onRetry` no-re-fire facet), exercised by the deterministic
  `RetryTimingAndExhaustionEdgesScenario` (`Inv21RetryTimingAndExhaustionEdgesTest`, seed 0 — backoff timing across
  crash/replay, the `retryWhile` bound, the `onRetry`-not-re-fired probe across crash/replay, and the per-attempt
  `execute` timeout → `TIMED_OUT`).

---

### INV-22: Event-name customization is sound
- **Name:** `EventNameCustomizationSound`
- **Kind:** Safety
- **Plain English:** A workflow registered with a custom `eventNameCustomizer` records its step/status events under the
  CUSTOMIZED wire names, those names are stable across crash/replay (a pure function of history), and the engine still
  routes/replays correctly under customization. Concretely, for a `CustomNamedWorkflow` registered with a
  `DefaultEventNameCustomizer` (custom namespace + workflowBaseName + at least one status-suffix override, e.g.
  `stepCompleted`): every committed step/status event of the instance carries the EXPECTED customized name/type
  (namespace + base + step + suffix per the customizer's rules); the instance reaches a terminal status; and a crash +
  replay reproduces the IDENTICAL customized names (replay re-runs the body but a present step yields a cached result
  and emits nothing — the recorded customized names are unchanged).
- **Formal-ish:** for each custom-named `workflowId` (those of the custom-named workflow, scoped to the `named-`
  prefix): let `customizer` be the registered `DefaultEventNameCustomizer`, and for each committed event `e` of the
  instance let `name(e) = e.MessageType.qualifiedName()` (the customized wire name the engine stamped on it). Then for
  every step event `e` (status `STARTED`/`COMPLETED`/…): `name(e) = (namespace, capitalize(stepName(e)) ·
  stepSuffix(status(e)))` (no step `baseName` is set, so the step base is the capitalized step name), and for every
  workflow-status event `e`: `name(e) = (namespace, capitalize(workflowBaseName) · workflowSuffix(status(e)))` — i.e.
  the customizer-derived expected `QualifiedName` exactly (namespace = the custom namespace, the status suffix the one
  the customizer carries — the `stepCompleted` override where it applies, the default otherwise). A non-empty but
  un-customized name (the default namespace, or a default `Completed` suffix where the override dictates `Done`), a
  missing suffix, or any mismatch is the break. Also: an issued start must produce a present instance carrying these
  customized names (never routed to none under customization) and the instance must reach a terminal workflow status.
  The replay-stability facet: `name(e)` for each `e` is a pure function of `history(w)`, so `replay(history(w))`
  reproduces the IDENTICAL customized name set — enforced jointly with INV-4 (`DeterministicReplay`), which already
  proves each instance's committed subsequence (including each event's stamped type) is reproduced verbatim; a replayed
  present step yields a cached result and emits nothing, so the recorded customized names are unchanged. Taken **per
  `workflowId`** (never pooled): the single global log interleaves independent instances non-deterministically (the F-2
  surface, §11), so another instance's (differently-named) event landing here is irrelevant; one instance carrying a
  wrong/un-customized name, or a replay producing a different name, is the break. Content-based (the per-event stamped
  `QualifiedName`), so robust to the F-2 intra-instance global-append-order non-determinism.
- **Engine mapping:** A custom `eventNameCustomizer` is set on the workflow via `WorkflowConfiguration.eventNameCustomizer()`
  (`WorkflowCustomization.eventNameCustomizer(...)`, set inside `.customized(...)`); the runtime inherits it per step via
  `EventNameCustomizer.forStepInheritance()` and applies it when naming every emitted event in `EventMessageUtils`: each
  `started/completed/failed/...Step` and `started/completed/...Workflow` helper calls
  `customizer.getEventName(stepName|workflowName, payload, status)` and builds the `GenericEventMessage`'s `MessageType`
  from the returned `QualifiedName` (`EventMessageUtils.java:87-88,106-107,192-193,217-222,…`). The
  `DefaultEventNameCustomizer.getEventName(stepName, params, stepStatus)` composes `namespace` +
  `capitalize(baseName ?? stepName)` + `stepStatusToName.get(status)`, and `getEventName(…, workflowStatus)` composes
  `namespace` + `capitalize(workflowBaseName ?? stepName)` + `workflowStatusToName.get(status)`
  (`DefaultEventNameCustomizer.java:284-327`); the `Builder.namespace(...)` static factory plus chained
  `.workflowBaseName(...)`/`.stepCompleted(...)` instance setters configure those maps (axon-flow-workflow skill §6.2).
  The customized name is therefore a deterministic function of `(stepName/workflowBaseName, status)` and the fixed
  customizer config; because the workflow body is deterministic (axon-flow-workflow skill §3.3) and a present terminal
  step yields a cached result and emits nothing on replay (`ExecuteDelegate.java:117`), a replay re-runs the body and
  re-stamps no new events — the recorded customized names are byte-for-byte stable. Changing the customizer's namespace
  on a shipped workflow is treated like a step rename (a forbidden migration, axon-flow-workflow skill §6.2 / anti-pattern
  list) — INV-22 pins that the engine *honours* the registered customizer, not that the customizer may change.
- **Currently holds?** Yes (honest assessment) — the engine stamps every emitted step/status event with the
  customizer-derived `QualifiedName` (the custom namespace + the customizer's base/suffix rules, including the
  `stepCompleted` override), the instance reaches a terminal status under customization, and because the body is
  deterministic + a replayed present step emits nothing, a crash + replay reproduces the IDENTICAL customized names. The
  DST harness asserts this after every step (including across the crash/restart/reorder faults, against a
  `CustomNamedWorkflow` registered with a custom customizer and driven alongside the order workhorse) by checking each
  committed event's `QualifiedName` equals the customizer-derived expected, and the dedicated scenario asserts a fresh
  start records the customized names, reaches terminal, and a crash + replay reproduces the identical names. No
  counterexample observed across the fuzz campaign. (A genuine break — a customized name NOT applied, a replay producing
  a DIFFERENT name, or a customized instance failing to route/terminate — would be a high-value finding, the customizer
  not honoured or replay diverging under customization, to be triaged per the POC rules, not silently tolerated.)
- **Checked by:** DST. **Scope decision (TLA+):** kept DST-only, exactly like INV-7..21. The Phase-2 TLA+ model
  (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and has **no notion of event-name
  customization, an `EventNameCustomizer`, a wire-level event name, a namespace, or status suffixes** — its `log` is a
  sequence of `<<step, status>>` step records with no wire name attached (`formal/tla/README.md` "Scope & limits").
  Modelling name customization would require adding a customizer config + a name-composition function + a per-event wire
  name, a whole new state concept outside that scope, bloating a model kept tiny so TLC finishes in seconds. INV-22 is an
  implementation/DST property (it is about the engine's event-naming behaviour across replay, not the leasing protocol),
  so it is asserted in the simulator only — `Invariants.assertEventNameCustomizationSound` (per `workflowId`, scoped to
  the `named-` id prefix, content-based / F-2-robust), exercised by `DstSimulation` (every step + a horizon pin, against
  a `CustomNamedWorkflow` registered with a custom customizer and driven alongside the order workhorse through the full
  fault set — folded into the fuzz) and the deterministic `EventNameCustomizationSoundScenario`
  (`Inv22EventNameCustomizationSoundTest`, seed 0).

---

### INV-23: Engine self-protection
- **Name:** `EngineSelfProtection`
- **Kind:** Safety
- **Plain English:** When code abuses the engine at one of its two known self-protection surfaces, the engine must not
  silently CORRUPT or TEAR an instance's committed history; the worst it does is stop making progress (a non-terminal
  stall the harness's bounded deadline catches) or throw a clean, surfaced exception. Two surfaces are probed end-to-end:
  **(1) Nested primitive** — a workflow whose `execute` action lambda calls another primitive (the
  `axon-flow-workflow` skill §3.1-forbidden pattern). PROBE result (empirically observed, both characterized): the engine
  has **no up-front guard** against the nested call — there is no re-entrancy check on `appendTask`. Whether it
  deadlocks or transparently works is decided entirely by the body executor's threading model, and is
  **non-deterministic** under thread-scheduling contention: under the engine's **default virtual-thread-per-task
  executor** (`WORKFLOW_ENGINE_EXECUTOR`) the nested primitive **usually does NOT deadlock** — the inner action runs on
  its own thread, the outer action's `awaitStateChange` consumes the inner's queued tasks, and both steps complete to a
  terminal COMPLETED workflow with no torn/duplicate records — but under contention the inner-vs-outer
  queue-consumption race can instead leave the instance **stalled** non-terminally (observed under full-suite load); under
  a **single-threaded body executor** the same nested primitive **deterministically DEADLOCKS** the per-instance task
  queue (the inner `appendTask` is enqueued but can never be consumed because the only consumer thread is the outer
  action blocked on it — in fact it parks already at the workflow-START await) — the instance reaches NO terminal status
  and the only committed event is the workflow `STARTED`. In **every** case the committed log is never corrupted — the
  silent-complete path completes cleanly, the stall/deadlock paths append nothing torn. The engine never
  *detects/rejects* the §3.1 misuse with a clear error (this is the candidate robustness gap **F-5**). **(2) Task-queue
  overflow** — driving more queued tasks at one instance than the per-instance bound (`taskQueue = new
  ArrayBlockingQueue<>(1000)`) makes `appendTask` fail CLEANLY: `offer` returns `false` and it throws
  `RuntimeException("Too many tasks to perform workflow instance")` **before** any event is published — it does not
  half-write, duplicate, or reorder committed history, and the already-queued ≤1000 tasks stay intact and consumable.
  The overflow surfaces as a clean failure, not corruption.
- **Formal-ish:** for the self-protection probe instance(s) (scoped to the `selfprot-` id prefix):
  *(nested primitive)* for each probed `workflowId`, the instance's committed subsequence is either (a) a well-formed
  completed history — exactly one `STARTED` workflow-status event, well-formed step records, and a terminal workflow
  status, with at most one terminal record per `(workflowId, stepName)` (the INV-2 shape) — **or** (b) a non-terminal
  prefix (no terminal workflow status; the instance still LIVE / stuck), with **no torn record**: `∄` a `COMPLETED`
  step record whose `STARTED` is absent, `∄` a duplicate terminal record, `∄` a step/status event for a step the body
  never recorded a `STARTED` for. A *corrupt* history — a torn/duplicate-terminal/orphan record — is the break. The
  per-instance ordering and counts are taken **per `workflowId`** (never by global index — the F-2 surface, §11).
  *(task-queue overflow)* let `bound = 1000` (the `ArrayBlockingQueue` capacity). Then appending the `(bound + 1)`-th
  pending task throws (a clean surfaced exception), and after the throw the queue still holds exactly the first `bound`
  tasks in FIFO order (`getNextTask()` returns them, none lost or duplicated), and no committed event was published by
  the failed `appendTask` (the throw precedes any `eventSink.publish`). A silent drop, a torn/duplicate committed
  record, or a corrupted queue is the break.
- **Engine mapping:** *(nested primitive)* the per-instance progress carrier is the single-threaded task queue
  `taskQueue` (`SimpleWorkflowExecution.java:91`, an `ArrayBlockingQueue<>(1000)`); the single consumer is
  `awaitStateChange` looping `taskQueue.take(); task.accept(this)` (`:344-352`) /
  `AbstractStepExecutor.acceptAllPendingTasksForStep` (`:102-110`). A primitive's body runs its action on the injectable
  body executor (`ExecuteDelegate.java:138-156`, `customize.workScheduler(executor)`); the inner `awaitExecute` re-enters
  `ExecuteDelegate.execute`, which `appendTask`s the inner STARTED (`:119`) and then `awaitStateChange`s on the inner
  step (`:123`). Under a multi-threaded body executor the inner action gets a distinct thread, so the inner queue tasks
  are consumed and both steps complete; under a same-thread executor the inner action cannot run while the outer action
  holds the only thread, so the inner STARTED task is never consumed and the queue deadlocks. There is **no** re-entrancy
  guard on `appendTask` (`:471-476`) that would reject the nested call — the skill's §3.1 "appending a task from within a
  task consumer blocks forever" is enforced only implicitly (and only under a single-threaded consumer). *(task-queue
  overflow)* `appendTask` is `if (!this.taskQueue.offer(task)) throw new RuntimeException("Too many tasks to perform
  workflow instance")` (`SimpleWorkflowExecution.java:471-476`): a clean throw on a full bounded queue, raised before the
  task (and hence before any `sendStepEvent`/`eventSink.publish` it would have triggered) runs. This is the §13.4
  task-queue bound; the `// FIXME <- task queue is full, backpressure?` flags that there is no backpressure, only the
  hard throw.
- **Currently holds?** Yes for the **no-corruption** safety property the invariant asserts (honest assessment): at both
  surfaces the committed history stays consistent — the overflow throws cleanly without publishing/tearing anything, and
  the nested primitive either completes cleanly (virtual-thread executor) or stalls non-terminally with nothing
  corrupted (single-threaded executor). It **documents** (does not patch) the candidate robustness gap **F-5**: the
  engine surfaces the §3.1 nested-primitive misuse only as a silent deadlock under a single-threaded body executor (or a
  silent success under the default executor), **never** as a clear up-front error — a common authoring mistake caught
  only by liveness/the bounded deadline, not by a guard. Per the POC rules the engine is **not** patched; INV-23
  documents the observed behaviour and pins the no-corruption contract. (A *corrupt* history at either surface — a torn
  or duplicate-terminal record on overflow, or a torn record on the nested-primitive stall — would be a high-value
  finding to triage per the POC rules, not silently tolerated.)
- **Checked by:** DST (deterministic scenario only; **not** in the per-step fuzz set — a nested-primitive instance
  deliberately stalls non-terminally under a single-threaded executor, which the liveness horizon check would read as a
  hang, the same reasoning that scenario-pins INV-9/16/17/18). **Scope decision (TLA+):** kept DST-only, exactly like
  INV-7..22. The Phase-2 TLA+ model (`WorkflowLeaseRecovery.tla`) is deliberately scoped to leasing + crash-recovery and
  has **no notion of the per-instance task queue, a body executor, the `appendTask` re-entrancy / queue-bound, or nested
  primitive calls** — its `log` is a sequence of `<<step, status>>` records with no queue or executor state. Modelling
  the task-queue mechanics + executor threading would introduce a whole new state concept outside that scope and bloat a
  model kept tiny so TLC finishes in seconds. INV-23 is an implementation/DST property (it is about the engine's
  self-protection at the task-queue surfaces, not the leasing protocol), so it is asserted in the simulator only —
  `Invariants.assertEngineSelfProtection` (per `workflowId`, scoped to the `selfprot-` prefix, content-based /
  F-2-robust: the committed history is either well-formed-complete or a clean non-terminal prefix, never corrupt) plus
  `Invariants.documentNestedPrimitiveNotGuarded` (records the observed outcome — silent-complete vs silent-deadlock,
  never a clean rejection — the F-5 candidate, mirroring `documentEffectAtMostOnceGap`), exercised by the deterministic
  `EngineSelfProtectionScenario` (`Inv23EngineSelfProtectionTest`, seed 0): the nested-primitive observed behaviour
  under both the default and the single-threaded executor, and the task-queue-overflow clean-failure contract driven
  against the **real** `SimpleWorkflowExecution.appendTask` (its real `ArrayBlockingQueue<>(1000)`).

---

### INV-24: A node does no work for a segment it does not hold
- **Name:** `NoWorkAfterRelease`
- **Kind:** Safety
- **Plain English:** At most one node holds a given segment's token at any time, every live node's belief about what it
  holds matches the token-store row, and a node stops acting on a segment the moment it no longer holds it — no
  instance of that segment is started, restored, evicted or advanced afterwards. The liveness twin the same protocol
  owes: a node must always be **able to boot** against a shared durable store, whoever else is running, and every
  segment must end up owned by somebody.
- **Formal-ish:** over nodes `n` and segments `s`, with `owner[s]` the token-store row and `held[n]` the node's belief:
  `∀ s : Cardinality({n : owner[s] = n}) ≤ 1` (exclusivity); `∀ n ∈ running, s ∈ held[n] : owner[s] = n`
  (agreement — a stale belief is the break); and `∀ s, n : executedFor(s) ⊆ heldAt(n, s)` — no execution attributed to a
  segment outside the interval in which its node held it. The liveness side: a boot attempt never terminates in
  `UnableToClaimTokenException`, and `◇ (∀ s : owner[s] ≠ none)`. A boot that fails, a segment nobody owns, or an
  execution after release is the break.
- **Engine mapping:** claims live in the processor's `TokenStore` (a durable `JdbcTokenStore`/`JpaTokenStore` in a
  multi-node deployment; the default `InMemoryTokenStore` keeps them process-local, which is F-1). Claims and releases
  are driven by the framework `Coordinator`; the engine sees them as
  `SegmentChangeListener.onSegmentClaimed(Segment)` / `onSegmentReleased(Segment)` and
  `Checkpointing.onSegmentClaimed(Segment, CheckpointTrigger)` / `onSegmentReleased(Segment, TrackingToken)`. The
  workflow module additionally performs a **pre-processor start scan** over every segment to compute the earliest token
  for its replay decision — the scan that produced **F-25/F-31/F-32** by claiming tokens it only needed to read.
  `releaseSegment` is scoped by `ownedBy(segment)` and was correct throughout.
- **Currently holds?** Yes for the claim protocol, after the F-25/F-31/F-32 fix: the scan reads without holding claims,
  skips segments owned by another node, and re-reads on a lost initialisation race. **One residual, stated:** the
  release path is model-confirmed to hand a segment over **before the losing node stops writing** — `releaseSegment`
  interrupts running-step futures and returns without draining, and the framework releases the claim *before* it
  notifies the engine at all (`Coordinator`, release-claim then `onSegmentReleased`). That is **S-9**, model-confirmed
  (double start and double completion in the model) but **not** reproduced by a test, and it is not the GC-pause zombie
  deferred to issue #271 — it is a window on every planned rebalance and rolling deploy.
- **Checked by:** **TLA+** — `formal/tla/sharding/SegmentClaim.tla`, operators `NoWorkAfterRelease` plus the companions
  `AtMostOneOwner` (segment scope — distinct from INV-1's instance scope), `ClaimAgreement`, `NoDuplicateExecution`,
  `BootNeverFails`, `NodeCanJoin`, `NoOrphanSegment`. Violated/fixed cfg pairs: `SegmentClaim_join_violated_seq`
  (196 distinct states) / `SegmentClaim_join_fixed_seq` (124 distinct) for the join and mutual-boot cases;
  `SegmentClaim_restart_violated` / `SegmentClaim_restart_fixed` for the crash-during-boot case;
  `SegmentClaim_release_violated` / `SegmentClaim_release_fixed` for the release window; `SegmentClaim_steal_*` for
  expiry and steal. The pairs differ by one boolean, so the green sides are non-vacuous.
  **Scope decision (DST):** **no DST assertion exists and none can today.** The harness models no segments and no
  nodes — `SimulationConfig` has no field for either, segment count cannot be set from outside the engine, and
  `InMemoryTokenStore` has a no-op `releaseClaim` and no owner column, so two in-JVM nodes cannot contend;
  `Invariants.assertAtMostOneOwner` is still called with a literal `1`, an arm that cannot fail. One JVM also shares
  `ClockUtils.instant()` and the default `pid@host` node identity, so every expiry/steal arm would pass vacuously.
  The invariant is therefore carried by TLA+ plus **real infrastructure**: `NodeJoinIT` (a cold node joins while a peer
  holds all four claims, claim rows unchanged at the moment it is ready, peer on a 60 s claim timeout so the join
  cannot be an expired claim), `MultiNodeStartupTest`, `WholeClusterRestartIT`, `ScaleDownAndUpIT`, `RollingDeployIT`,
  `StoreOutageIT`, plus mocked-`TokenStore` unit tests on the scan. Findings: **F-25**, **F-31**, **F-32** (fixed);
  **S-9** (open, model-confirmed only).

---

### INV-25: Claim and release act only on the instances the segment owns
- **Name:** `OneInstanceOneOwner`
- **Kind:** Safety
- **Plain English:** Every pass that walks the workflow-execution repository on a segment lifecycle event — restoring
  instances on claim, starting restored instances when a segment reaches its replay boundary, evicting terminal
  instances, releasing on hand-over — touches **only** the instances that segment owns. Claiming segment B must never
  start, evict or act on an instance owned by segment A, and only the owning segment ever acts on an instance.
- **Formal-ish:** with `Owner[w]` the segment a workflow id routes to and `actedBy[w]` the set of segments that have
  acted on it: `∀ w : actedBy[w] ⊆ {Owner[w]}`. Equivalently, for every repository pass triggered by segment `s`, the
  set of instances it starts, removes or seeds is a subset of `{w : Owner[w] = s}`. An instance started by two segments
  — which then also produces a second `<workflow>` history for one id — is the break.
- **Engine mapping:** `removeTerminalAndStartRestoredWorkflowExecutions` took **no `Segment`** and both its predicates
  were repository-global (`removeAll(isTerminal)` and `findAll(!isRunning)`), while `releaseSegment` next to it was
  correctly scoped by `ownedBy(segment)` throughout. It is reached from **two** call sites — `claimSegment` and
  `onLiveModeActivated` — and both had to be scoped: a segment reaching the replay boundary starts another segment's
  instances just as a claim does. Routing itself is `SegmentedWorkflowRouting` / `shouldHandle(workflowId, segment)`.
- **Currently holds?** Yes, after the **F-27** fix scoped both predicates at both call sites, mirroring
  `releaseSegment`. Unscoped was *equivalent to* scoped while there was exactly one segment; this is inherited
  single-segment code that sharding made wrong, not new code. Note the interaction the model made explicit: because the
  coordinator can claim segments in parallel, the unscoped pass let B's claim start A's freshly rehydrated,
  not-yet-started instances, after which A's own claim started them a second time.
- **Checked by:** **TLA+** — `formal/tla/sharding/WfShard.tla`, operator `OneInstanceOneOwner`. `cfg_d2_owner` violates
  it at **317 distinct states** with only `claimSegment` scoped, which is how the second call site was found; the
  planned fix was corrected before it was written. `cfg_d2` / `cfg_fixed` are the paired arms.
  **Scope decision (DST):** none — see INV-24's scope note (the harness has no segment dimension). Carried instead by
  the unit test `WorkflowEngineSegmentClaimStartScopeTest`
  (`claimingASegmentDoesNotStartTheWorkflowExecutionsOwnedByAnotherSegment` and
  `aSegmentReachingTheReplayBoundaryStartsOnlyItsOwnWorkflowExecutions` — one per call site), written first as an
  expected-gap test that passed while asserting the bug was present, observed flipping **red** when the real fix landed,
  and only then inverted. Real-infrastructure companions: `CrossSegmentSpawnIT`, `ScaleDownAndUpIT`. Finding: **F-27**
  (fixed).

---

### INV-26: No workflow body runs on a segment that has not caught up
- **Name:** `NoWorkWhileReplaying`
- **Kind:** Safety
- **Plain English:** Replay state is **per segment**. A workflow body — with its real side effects — runs only once
  *the segment handling it* has reached the startup boundary, and a restored execution is seeded with **its own**
  segment's position. One segment catching up must not put the whole engine live, and a claim-time restore must not
  start a body at event-store head while that segment's token is still behind.
- **Formal-ish:** with `live[s]` per segment and `pos[s]` its position: `∀ w, s : executes(w, s) ⇒ live[s]`, and for a
  restored execution `seed(w) = pos[Owner[w]]` — never `pos[s']` for some other `s'`. A body executing under
  `¬live[Owner[w]]`, or seeded from another segment's token, is the break. The **liveness twin** matters as much and is
  checked with it: an instance on a segment handed to a node must eventually run — a gate that defaults to "not live"
  on a claim parks it forever.
- **Engine mapping:** `WorkflowEngineReplaySupport` held a single `liveMode` flag and a single `currentTrackingToken`;
  every event on every segment overwrote the token, and the first segment to reach the startup latest token flipped the
  flag for the whole engine, which `WorkflowEngine` then read to gate spawning. Separately, `claimSegment` never read
  live mode at all, so `loadRunningWorkflows` sourced from the event-store head and started the body immediately.
- **Currently holds?** Yes, after **F-28** and **F-33** were fixed **together** — both are per-segment now, and
  claim-time restore defers the bodies of a segment known to be behind. The model proved they must not be split: fixing
  F-33 alone while live mode stays engine-wide is a *liveness regression*, because a segment handed to a node whose
  `liveMode` is still false leaves the instance idle forever; F-33 was *masking* F-28's migration hazard, so fixing one
  in isolation trades a safety bug for a hang. **Two residuals, stated:** (1) the claim-time gate cannot read the
  segment's position, because `SegmentChangeListener.onSegmentClaimed(Segment)` omits the token (**FW-2**), so it gates
  on *positive evidence of lag* instead — deliberately asymmetric with the spawn gate, which defaults to "not live"
  because a false negative there only defers a body while a false negative on claim parks it forever; (2) consequently
  **cross-node migration still starts restored bodies at head**, because a node that never held the segment has no
  local evidence of lag. That residual is blocked upstream on FW-2, not on this repo.
- **Checked by:** **TLA+** — `formal/tla/sharding/WfShard.tla`, operator `NoWorkWhileReplaying`, with the liveness twin
  `MigratedInstanceMakesProgress`. `cfg_live_asis` / `cfg_live_fixed` / `cfg_live_fixed3` are the safety arms, and
  `cfg_live_trap` is the arm that matters: it reports **temporal properties violated, State 17 stuttering, 9,154
  distinct states** — the hang produced by the fix as originally planned.
  **Scope decision (DST):** none — see INV-24's scope note. Carried by the unit tests
  `WorkflowEngineSegmentLiveModeScopeTest`
  (`noWorkflowBodyRunsOnASegmentThatHasNotReachedTheStartupLatestToken` and
  `aRestoredExecutionIsSeededWithTheTokenOfItsOwnSegment`; the lagging segment's token is asserted demonstrably not to
  cover the boundary, so the arm cannot pass vacuously) and `WorkflowEngineClaimDuringReplayTest`
  (`reclaimingASegmentThatIsStillReplayingDoesNotRunTheRestoredBodyAtHeadState`). Findings: **F-28**, **F-33** (fixed).

---

### INV-27: A segment's stored token never passes an event that segment has not applied
- **Name:** `TokenNeverPassesUnappliedEvent`
- **Kind:** Safety
- **Plain English:** Checkpointing is **per segment**. The token stored for a segment advances only past events that
  segment itself has handled and whose effects are durable: a checkpoint requested for one segment must never advance
  another's token, and a segment's token must not pass an event whose wake or completion is still sitting in some
  instance's queue — including an instance that has been materialized but whose body has not started, and including the
  moment the engine is shutting down. Violating it is **silent event loss**: on restart the segment resumes past events
  it never handled, with no error and no log line.
- **Formal-ish:** for each segment `s`, `storedToken[s] ≤ min(appliedPos[s], barrier[s])`, where `appliedPos[s]` is the
  furthest position `s` has handled and `barrier[s]` the lowest position of any outstanding queued effect on an
  instance owned by `s`. A request routed to `s' ≠ s`, an advance past a queued-but-unconsumed match, or an advance
  during shutdown with a pending wake outstanding are each the break. The consequence to check alongside it: no
  committed wake is dropped (`NoSilentDrop`) and a wake survives the hand-over (`WakeSurvivesHandover`).
- **Engine mapping:** `WorkflowEngineCheckpointingSupport` held one `CheckpointTrigger` for all segments, discarded the
  `Segment` handed to `onSegmentClaimed`, nulled the field on any segment's release, and coalesced
  `pendingCheckpointToken` across segments with `upperBound` — while `hasPendingCheckpointWork(Segment)` and
  `scheduleCheckpointIntent(Segment, …)` beside it were already segment-scoped. The barrier itself lives in
  `WorkflowExecutionCheckpointSupport` (`hasUnsafeCheckpointWork` / `appendCheckpointIntent`), and the delivery-scoped
  protection is `handle` calling `onEvent` — which **enqueues** the match — before requesting the checkpoint. Shutdown
  ordering is a lifecycle-phase question in `WorkflowConfigurationDefaults`.
- **Currently holds?** Yes, after **F-26** (triggers keyed by segment), **F-35** (the engine shuts down strictly after
  the processor has drained, instead of racing it in the same phase) and **F-36** (a materialized-but-not-started
  execution now queues the match *and* counts towards the barrier — two halves, and neither works alone: fixing only
  the first queues work the token may pass, fixing only the second holds the token back for a match never queued).
  **Two things it does not cover:** the framework does not defend the stored token against an over-high request — it
  stores it verbatim rather than clamping to `lastConsumedToken` (**FW-4**), which is what gave F-26 its blast radius
  and what will give the next caller the same one; and **F-34b** is outside this invariant, because there the wait was
  never durably published, so there is no barrier to hold.
- **Checked by:** **TLA+** — `formal/tla/sharding/Holdback.tla`, operators `TokenNeverPassesUnappliedEvent`,
  `NoSilentDrop`, `WakeSurvivesHandover`, `NoDuplicateCompletion`. Arms: `C1_baseline`,
  `C2_TokenNeverPassesUnappliedEvent` / `C2_NoSilentDrop` / `C2_short_claim_asworks` against `C3_short_claim_fixed`,
  `C3b_short_claim_fullfix` and `C3c_holdbackfix_only`; `C4_shutdown_clears_first` against
  `C4b_shutdown_race_with_fixes` for the shutdown race, with `C6_liveness` / `C7_liveness_shutdownrace` for the
  liveness side; `C5_partial_drain`, `C8_two_instances_one_segment`; and `M1_no_holdback` /
  `M2_single_round_barrier` / `M3_restore_ignores_completion` as mutation arms proving the green sides non-vacuous.
  **Scope decision (DST):** none — see INV-24's scope note. Carried by the unit tests
  `WorkflowEngineCrossSegmentCheckpointTest`
  (`asyncCheckpointOfOneSegmentLeavesTheStoredTokenOfAnotherSegmentAtItsOwnPosition`, parameterized over fully-deferred
  and auto checkpointing so the mode cannot be blamed, driving one real framework `CheckpointingProgressStrategy` per
  segment and reading the assertion back out of a real token store; plus
  `aCheckpointRequestReachesOnlyTheTriggerOfItsOwnSegment` and `releasingOneSegmentKeepsTheOtherSegmentsCheckpointing`),
  `WorkflowConfigurationDefaultsTest#workflowEngineShutsDownAfterTheEventProcessorHasDrained`, and
  `WorkflowEngineReplayTest` for the queued-match half of F-36. Real-infrastructure companion:
  `DurableWaitAcrossRebalanceIT` — five `kill -9` runs, three graceful runs over three rounds, one control,
  ~1000 instance-level observations, **zero losses**. Findings: **F-26**, **F-35**, **F-36** (fixed); **FW-4**
  (upstream).

---

### INV-28: An instance is spawned and woken exactly once, by the segment that owns it
- **Name:** `SpawnExactlyOnce`
- **Kind:** Safety
- **Plain English:** Placement is a pure function of the routing key, not of which node happens to be warm, which node
  holds the segment, or how the event reached the engine. A start event admitted for an instance creates **exactly
  one** execution, on the one segment its id maps to; every wake due to a live instance lands **exactly once**; and
  neither count changes when the same business event is delivered twice, when it is broadcast to every segment rather
  than hash-routed, or when the segment moves between nodes. A durable timer that survives the move fires on its
  original deadline, not a restarted one.
- **Formal-ish:** `∀ w : spawned[w] = spawnSeen[w]` and `∀ w : doneWakes[w] = dueWakes[w]`, with
  `spawned[w], doneWakes[w] ∈ {0, 1}` per due event and every contributing action attributed to `Owner[w]`; plus
  `∀ w : commits[w] ≤ 1` (no double completion) and `¬ ∃ w` live on two nodes at once. A second execution for one id, a
  wake applied twice or dropped, or a completion committed twice is the break.
- **Engine mapping:** routing is `SegmentedWorkflowRouting` — `shouldHandle(workflowId, segment)` on the handling path
  and `shouldSpawn` on the spawn path, over a key derived by the `workflowIdProvider`. Broadcast events (those whose
  spawn candidates cannot be derived) go to every segment and are filtered by those guards rather than by routing.
  Deadlines are recomputed from the **durable step timestamp**, not from arming time, which is what makes a timer
  survive a hand-over.
- **Currently holds?** Yes, and this is the campaign's strongest precise negative rather than a defect. Two caveats that
  are *not* this invariant: an instance may still be started twice by an **unscoped repository pass** (INV-25 / F-27,
  fixed) or by the release-before-drain window (S-9, open) — both are ownership failures, not routing failures. The
  routing guards themselves carry it. Also note **S-10**, untested: `#` is silently load-bearing in the segment key
  (everything before the first `#`), so a `workflowIdProvider` returning `order#123` collapses every order onto one
  segment and sharding silently degrades to one segment. That is a placement *soundness* hazard with no validation and
  no test.
- **Checked by:** **TLA+** — `formal/tla/sharding/WfShard.tla`, operators `SpawnExactlyOnce` and `WakeExactlyOnce`,
  with `NoDoubleStart` and `AtMostOneCompletion` alongside. The headline run: both hold with **all four modelled
  defects enabled**, at 3 segments / 3 instances / **1,722,131 distinct states, depth 31** (`cfg_asis3_spawn`,
  `cfg_asis3_wake`, `cfg_fixed3`); deleting the `shouldHandle`/`shouldSpawn` guards makes both fail within **8 steps**
  (`cfg_mut`, `cfg_mut_wake`), so the green is real and not vacuous. **This closes off a whole line of enquiry: the
  duplication risk lives in the release/claim window, not in broadcast routing.**
  **Scope decision (DST):** none — see INV-24's scope note. Carried on **real infrastructure** by
  `DuplicateStartEventsIT` (8 duplicate starts and 8 duplicate resumes committed; effect once, on both the hash-routed
  and the broadcast path), `TimerAcrossHandoverIT` (a 60 000 ms timeout fired at 60 110 ms from arming on the node that
  *took* the segment, and retry backoff at 30 018 ms from the first attempt — timers **resume**, they are neither
  restarted nor dropped), `IdleClusterSparseTrafficIT` (after a 120 s idle gap placement still routes by hash, not to
  the warm node; maximum claim age 6 014 ms against a 10 s timeout, owners unchanged — claim renewal is **not**
  event-driven), `CrossSegmentSpawnIT` and `SustainedLoadHandoverIT`. No finding — recorded as a set of precise
  negatives so they are not re-hunted.

---

## Cross-reference contract

This is the bridge Phase 5 verifies and locks: each `MachineName` appears verbatim as the named TLA+
operator/property (`formal/tla/WorkflowLeaseRecovery.tla`) and as the named DST assertion
(`simulation/.../invariants/Invariants.java`). The TLA+ and DST cells below are the **landed**
references (cfg that exercises the operator → `Invariants.assertX` / `documentX` method → DST
scenario/test class + seed). The plain-English statement of each invariant matches verbatim across
this file, the `.tla` operator comment, and the `assertX` Javadoc (verified in Phase 5).

A co-located, run-by-run bridge table (TLA+ cfg → INV → scenario → test → seed → outcome) lives in
[`formal/tla/README.md`](tla/README.md#tla--dst-bridge).

**INV-24…INV-28 (the sharding set) break the bridge in the other direction, deliberately.** Their `MachineName`s are
operators in the sharding models under `formal/tla/sharding/` (`SegmentClaim.tla`, `WfShard.tla`, `Holdback.tla`), not
in `WorkflowLeaseRecovery.tla`, and **none of them has a DST assertion**: the harness models no segments and no nodes,
so there is nothing to assert against (see INV-24 "Scope decision" for the full rationale). They are carried by TLA+,
by unit tests in `runtime`, and by the multi-JVM rig — the last of which is never reproducible by seed, so its evidence
carries the weaker label *confirmed on real infrastructure*. Closing the gap means adding a segment/node dimension to
`SimulationConfig`; that is the largest outstanding item on this file.

| INV | MachineName | Kind | Holds? | Finding | TLA+ operator + cfg (`formal/tla/`) | DST assertion (`Invariants.*`) | DST scenario · test · seed(s) |
|---|---|---|---|---|---|---|---|
| INV-1 | `AtMostOneOwner` | Safety | Partial | F-1 | `AtMostOneOwner`; holds in `MC_safe`/`*_fixed`, **VIOLATED** in `MC_owner.cfg` (non-durable lease) | `assertAtMostOneOwner` (≤1 owner); `documentSplitBrainOwnership` (expects ≥2) | `SplitBrainScenario` · `F1SplitBrainTest` · seed 0 |
| INV-2 | `AtMostOnceRecording` | Safety | Yes (given INV-1) for `execute`/`waitFor`/`migrateVersion`/`modifyPayload` (**F-7 FIXED**); **does NOT cover workflow-status terminals (F-13 coverage gap)** | F-1 consequence; **F-7 — FIXED** (`modifyPayload` now gates its publish on `!containsStep`, so it no longer re-records on a post-crash live re-run); **F-13 coverage gap** (counts only step-status terminals — a duplicate `<workflow>:CANCELLED` is NOT caught here, only by INV-7) | `AtMostOnceRecording`; holds in `MC_safe`/`MC_record_fixed`, **VIOLATED** in `MC_record.cfg` (split-brain, no append-condition); `PayloadRepublish.tla` `MC_payload.cfg` **VIOLATED** (old F-7 re-publish) / `MC_payload_fixed.cfg` → `No error` (the `!containsStep` gate) | `assertAtMostOnceRecording` (≤1 terminal *step* record; skips workflow-status events); `documentDuplicateRecordingUnderSplitBrain` (expects ≥2) | `WriteThenVanishScenario` asserts INV-2 still holds (seeds 1,2,3); `SplitBrainScenario` · `F1RecordDuplicationTest` · seed 0 shows the split-brain duplicate; `DuplicateTerminalPayloadRecordScenario` · `F7DuplicatePayloadRecordTest` · seed 0 confirms the fixed `modifyPayload` keeps the count at 1 / INV-2 holds across crash/recover (F-7 FIXED) |
| INV-3 | `CommittedHistorySurvivesCrash` | Safety | Yes | — | `CommittedHistorySurvivesCrash` PROPERTY `[][Crash ⇒ log'=log]`; **No error** (`MC_safe.cfg`) | `assertCommittedHistorySurvivesCrash` (post-recovery log ⊇ committed, order preserved) | exercised across the crash/recover faults in the DST harness (`DstSimulation`) |
| INV-4 | `DeterministicReplay` | Safety | Partial | F-2 | `DeterministicReplay`; **No error** at model scope (`MC_safe.cfg`) — F-2 risk surface abstracted out | `assertDeterministicReplay` (per-instance replay-twice **multiset** equality — content, not F-2 global-append order) | replay-twice check in the DST harness (`DstSimulation` / `DstReproduceTest`) |
| INV-5 | `EventuallyTerminates` | Liveness | Partial | — | `EventuallyTerminates` temporal `◇` under `FairSpec`; **No error** (`MC_live.cfg`) | `assertEventuallyTerminates` (no live instance at virtual-time horizon) | virtual-time-horizon check in the DST harness (`DstSimulation`) |
| INV-6 | `EffectAtMostOnce` | Safety | **Yes (F-0 FIXED)** — at-most-once via skip-and-resolve: an in-flight attempt found STARTED on resume is not re-run; it resolves through the regular error flow (no retry → FAILED w/ `StepIndeterminateException`; retry → RETRYING). Strict per-step on the no-retry path; per-attempt for retried steps (see adoc caveat) | **F-0 (fixed)** | `EffectAtMostOnce`; **VIOLATED** in `MC_effect.cfg` (old behaviour, crash in STARTED→COMMIT), **No error** in `MC_effect_fixed.cfg` (`APPEND_CONDITION=TRUE`) | `assertEffectAtMostOnce` (≤1, now the **live** check) | `WriteThenVanishScenario` · `F0EffectDuplicationTest` · seeds 1,2,3 (effect == 1, step resolves to FAILED) |
| INV-7 | `TerminalIsFinal` | Safety | Yes for crash/replay finality of NEW work; **No for the cancel-path workflow-terminal duplicate (F-13)** | **F-13** (cancel path re-publishes a duplicate `<workflow>:CANCELLED` across a crash/replay — duplicate durable workflow-terminal record; the cancel-path / workflow-terminal analogue of F-7); F-3 (start-event redelivery restarts a terminated key) | DST-only (model scope is leasing + crash-recovery; the spec `log` has no workflow-status events — see INV-7 "Scope decision") | `assertTerminalIsFinal` (per-`workflowId`: no NEW work after terminal — a *different* workflow status, or a step with no pre-terminal event; a late-appended event of a step that began before terminal is tolerated as the F-2 append-order artifact, AND a re-published *identical* terminal workflow status is tolerated as the F-13 gap) | `TerminalIsFinalScenario` · `Inv7TerminalIsFinalTest` (incl. late-COMPLETED + late-STARTED tolerance pins seeds 252/18, the F-3 redelivery pin, and the F-13 identical-terminal-status tolerance pin) · seed 0; `DuplicateCancelTerminalRecordScenario` · `F13DuplicateCancelRecordTest` · seed 0 shows the cancel-path workflow-terminal duplicate (F-13, count==2); also asserted every step in `DstSimulation` |
| INV-8 | `RetryBound` | Safety | Yes (record bound HOLDS; **F-9** (S-3) backoff-overflow liveness wedge ✅ **FIXED** — `exponential` clamps to `max` before any overflowing/negative/wrapped shift or multiply, so the step exhausts and goes terminal) | — | DST-only (model scope excludes retries/backoff — see INV-8 "Scope decision" and `formal/tla/README.md` "Scope & limits") | `assertRetryBound` (per-`(workflowId, stepName)`: ≤ maxRetries+1 attempt records) | `Inv8RetryBoundScenario` · `Inv8RetryBoundTest` · seed 0; also asserted every step in `DstSimulation` against `OrderWorkflow.shipOrder`; backoff-overflow FIX (F-9/S-3) pinned by `S3BackoffExponentialOverflowTest` · `BackoffOverflowScenario` · seed 0 (scenario-only — asserts the clamped schedule retries to exhaustion + terminal `FAILED` + exact `maxRetries + 1` bound) |
| INV-9 | `TimeoutsFire` | Safety | Yes (recording facet); surfacing facet **now consistent — F-8 FIXED on main** | **F-8 (fixed on main)** — all blocking-convenience timeout paths now surface `StepTimedOutException` (a subtype of `StepFailedException`); `resolveStepPayload` special-cases `result.timeout()` across all overloads | DST-only (model scope excludes time/timeouts — see INV-9 "Scope decision" and `formal/tla/README.md` "Scope & limits") | `assertTimeoutsFire` (per-`(workflowId, stepName)`: window elapsed ⇒ terminal step record); **surfacing facet** is a CHARACTERIZING pin (no assert helper): the actual surfaced exception type per blocking-convenience path | `Inv9TimeoutsFireScenario` · `Inv9TimeoutsFireTest` · seed 0 (recording facet) — **deterministic scenario only; not in the per-step fuzz set due to the `orTimeout` residual (Phase-3 D5)**; `BlockingAwaitTimeoutSurfaceScenario` · `F8BlockingAwaitTimeoutSurfaceTest` · seed 0 (surfacing facet — **F-8 fixed**: all four blocking-convenience paths → `StepTimedOutException`) |
| INV-10 | `OneInstancePerStart` | Safety | Yes (live case; F-3 is the after-terminal exception) | F-3 (after-terminal re-spawn, tolerated) | DST-only (model scope is a single instance / leasing + crash-recovery, with no spawn-dedup routing across business keys — see INV-10 "Scope decision") | `assertOneInstancePerStart` (per-`workflowId`: no second workflow-status `STARTED` without an intervening terminal status; tolerates the F-3 after-terminal re-spawn) | `OneInstancePerStartScenario` · `Inv10OneInstancePerStartTest` · seed 0; also asserted every step in `DstSimulation` |
| INV-11 | `VersionRoutingSound` | Safety | Yes | — | DST-only (model scope is a single instance / leasing + crash-recovery, with no workflow versions, multi-version registry, or start→definition routing — see INV-11 "Scope decision") | `assertVersionRoutingSound` (per-`workflowId`, scoped to the versioned id prefix: exactly one definition version per instance, equal to the highest registered version; an issued start producing no instance = routed to 0; deterministic across replay jointly with INV-4) | `VersionRoutingSoundScenario` · `Inv11VersionRoutingSoundTest` · seed 0; also asserted every step in `DstSimulation` against a two-version `VersionedOrderWorkflow` |
| INV-12 | `MigrateVersionContract` | Safety | Yes | — | DST-only (model scope is leasing + crash-recovery, with no workflow versions, version map, or `migrateVersion` primitive — see INV-12 "Scope decision") | `assertMigrateVersionContract` (per-`workflowId`, scoped to the migrating id prefix: the recorded migration version for a `changeId` is written at most once, monotonic non-decreasing / never downgraded, a valid semver; stable across replay jointly with INV-4) | `MigrateVersionContractScenario` · `Inv12MigrateVersionContractTest` · seed 0; also asserted every step in `DstSimulation` against a `MigratingOrderWorkflow` whose body calls `ctx.migrateVersion` |
| INV-13 | `NoLostPayloadWrites` | Safety | Yes | — | DST-only (model scope is leasing + crash-recovery, with no workflow payload, payload reducers, or `setPayload`/`modifyPayload` — see INV-13 "Scope decision") | `assertNoLostPayloadWrites` (per-`workflowId`, scoped to the payload id prefix: every committed step's payload contribution is reflected in the engine's reconstructed final payload unless a later same-key step overwrote it; stable across replay jointly with INV-4) | `NoLostPayloadWritesScenario` · `Inv13NoLostPayloadWritesTest` · seed 0; also asserted every step in `DstSimulation` against a `PayloadOrderWorkflow` whose every step writes a distinct payload key |
| INV-14 | `CombinatorConsistency` | Safety | Yes (decision facet HOLDS; surfaced 🟠 **F-10** (S-5) `anyMatch` no-match-all-completed winner-result edge, minor) | — | DST-only (model scope is leasing + crash-recovery, with no combinators, parallel branch steps, a predicate over step results, or the `matched()`/`unmatched()` categorization — see INV-14 "Scope decision") | `assertCombinatorConsistency` (per-`workflowId`, scoped to the combinator id prefix: each combinator's recorded decision — the distinct post-combinator step name + the engine's reconstructed-payload boolean — equals the decision the documented short-circuit semantics dictate over the branches' committed terminal outcomes; stable across replay jointly with INV-4) | `CombinatorConsistencyScenario` · `Inv14CombinatorConsistencyTest` · seed 0; also asserted every step in `DstSimulation` against a `CombinatorWorkflow` running three parallel branches folded through anyMatch/allMatch/noneMatch; the `anyMatch` no-match winner-result edge (F-10/S-5) pinned by `S5AnyMatchNoMatchWinnerTest` · `AnyMatchNoMatchWinnerScenario` · seed 0 (scenario-only) |
| INV-15 | `EventCorrelationExact` | Safety | Yes | — | DST-only (model scope is leasing + crash-recovery, with no event correlations, associations, a `waitForEvent` association key, or multi-instance event routing — see INV-15 "Scope decision") | `assertEventCorrelationExact` (per-`workflowId`, scoped to the correlated id prefix: every instance that completed its key-correlated wait recorded `matchedKey == its own key` — no cross-wakeup — and completed the wait at most once for the right key; uncorrelated/duplicate events produce no spurious completion; stable across replay jointly with INV-4) | `EventCorrelationExactScenario` · `Inv15EventCorrelationExactTest` · seed 0; also asserted every step in `DstSimulation` against two `CorrelatedWaitWorkflow` instances waiting on distinct keys |
| INV-16 | `FailurePropagation` | Safety | Yes | — | DST-only (model scope is leasing + crash-recovery, with no workflow-level FAILED status, step-failure status, or `ctx.fail` terminal primitive — see INV-16 "Scope decision") | `assertFailurePropagation` (per-`workflowId`, scoped to the failing id prefix: a terminal-failed step ⇒ the instance reached terminal `FAILED` (never `COMPLETED`), and no step after the failing one began; content-based / F-2-robust; stable across replay jointly with INV-4 and INV-7) | `FailurePropagationScenario` · `Inv16FailurePropagationTest` · seed 0 — **deterministic scenario only; not in the per-step fuzz set (scenario-pinned like INV-9)** |
| INV-17 | `StatusHookFiresOncePerStatus` | Safety | At-most-once / no-re-fire facet: **Yes** (the F-0 analogue holds); terminal COMPLETED at-least-once: **Yes** (F-4 FIXED — no longer dropped) | **F-4 — FIXED** (terminal COMPLETED hook no longer dropped; the happy completion path now `awaitStateChange`s on the terminal status before `finishWorkflow`, symmetric with the fail/cancel/timeout paths) | DST-only (model scope is leasing + crash-recovery, with no workflow status-change listeners, `@WorkflowStatusChangedHandler` registration, or the `setStatus`→`notify` callback path — see INV-17 "Scope decision"); standalone `HookOnCompletion.tla` corroborates the fix (`MC_hook.cfg` violated, `MC_hook_fixed.cfg` `No error`) | `assertStatusHookFiresOncePerStatus` (per-`(workflowId, status)`, scoped to the `hook-` id prefix: every registered status's hook fired ≤1 — at-most-once / no-re-fire across crash/replay) + `assertStartedHookFiredExactlyOnce` (STARTED == 1) + `assertCompletedHookFiredExactlyOnce` (COMPLETED == 1 — F-4 FIXED); `documentTerminalHookMayBeDropped` (retained as the re-fire guard / drop detector; post-fix always false) | `StatusHookFiresOncePerStatusScenario` · `Inv17StatusHookFiresOncePerStatusTest` · seed 0 — **deterministic scenario only; not in the per-step fuzz set (scenario-pinned like INV-9/16)** |
| INV-18 | `DriftGuardPausesCleanly` | Safety | Yes (empirically confirmed by live drift induction) | — (the drift-pause is the documented INV-5 non-termination carve-out) | DST-only (model scope is leasing + crash-recovery, with no runtime step-reference book, the `guardAgainstReplayDrift`/`unreferencedTerminalSteps` comparison, workflow versions, or the `ctx.migrateVersion` primitive — see INV-18 "Scope decision") | `assertDriftGuardPausesCleanly` (per-`workflowId`, scoped to the `drift-` id prefix, content-based / F-2-robust: a drift-paused instance recorded NO terminal workflow status, NO spurious drifted-step event, and its committed history is intact — a per-instance multiset superset of the pre-drift snapshot; the INV-3 durability contract) | `DriftGuardPausesCleanlyScenario` · `Inv18DriftGuardPausesCleanlyTest` · seed 0 — **induces drift LIVE** (v1-recorded instance replayed under a structurally-divergent v2 body via `crashAndRecoverWith`, tripping `WorkflowReplayDriftException`), then asserts the clean pause + non-trivial pins; **deterministic scenario only; not in the per-step fuzz set (scenario-pinned like INV-9/16/17)** |
| INV-19 | `PayloadReducerSemantics` | Safety | Yes (supported space); edge (a) null-value → **F-6** (candidate), generalized → **S-4** | **F-6** (candidate — a `null` value under `combine` wedges the instance: `Map.copyOf` rejects it, the read NPE is not turned terminal) **generalized by S-4** (ANY unexpected `RuntimeException` from a between-primitives user lambda — e.g. a throwing `modifyPayload` modifier — hits the same `handleWorkflowException` `default`-branch sink and wedges; ties to INV-5 carve-out + INV-16) | DST-only (model scope is leasing + crash-recovery, with no workflow payload, payload reducers, the three reducer behaviours, or the parameter-vs-result reducer distinction — see INV-19 "Scope decision"; extends INV-13) | `assertPayloadReducerSemantics` (per-`workflowId`, scoped to the `reducer-` id prefix, content-based / F-2-robust: the engine's reconstructed payload equals the documented reducer fold of the committed log — `global_only` discards the result, `combine` merges key-by-key, `local_only` replaces wholesale, `parameterPayloadReducer` governs the input view independently of the result write; last-writer-wins on a same-key combine; a combine that omits a key keeps the prior global value; stable across replay jointly with INV-4) | `PayloadReducerSemanticsScenario` · `Inv19PayloadReducerSemanticsTest` · seed 0; also asserted every step + a horizon pin in `DstSimulation` against a `ReducerWorkflow` whose steps exercise all three reducers + edges (b) parameter-vs-result interplay / (c) last-writer-wins / (d) missing-key (combine seed, local_only replace, combine, global_only, parameterPayloadReducer view, global_only-interplay, last-writer-wins pair), folded into the fuzz like INV-13; edges (a) null-value (**F-6**, characterized via `runNullEdge`/`ReducerWorkflow.nullEdge`) + the **S-4** generalization (throwing modifier, characterized via `runThrowingModifierEdge`/`ReducerWorkflow.throwingModifier`) + (e) replay-stability scenario-pinned (kept out of the always-on fuzz) |
| INV-20 | `VersioningEdges` | Safety | Yes | — | DST-only (model scope is a single instance / leasing + crash-recovery, with no workflow versions, multi-version registry, the `migrateVersion` primitive, a downgrade-rejection rule, or the multi-pass replay-routing lookup — see INV-20 "Scope decision"; extends INV-11 + INV-12) | `assertVersioningEdges` (per-`workflowId`, scoped to the `vedge-` id prefix: a downgrade `changeId` is never recorded and no version stamped below the started one; each `changeId`'s marker is written at most once and the recorded versions monotonic non-decreasing / valid semver; a fresh spawn is at the highest registered version; an instance recovered under a reduced registry resolves soundly — closest-sibling routing found a runnable body, never 0/not 2; stable across replay jointly with INV-4) | `VersioningEdgesScenario` · `Inv20VersioningEdgesTest` · seed 0 (fresh-spawn + downgrade-rejection + multi-`changeId` + crash-replay-stable, **plus** deeper closest-sibling routing via `crashAndRecoverWith` a reduced registry — the way INV-18 induces drift); also asserted every step + a horizon pin in `DstSimulation` against a three-version `VersioningEdgesWorkflow` whose body does two forward `ctx.migrateVersion` bumps + a rejected downgrade, folded into the fuzz |
| INV-21 | `RetryTimingAndExhaustionEdges` | Safety | Yes (onRetry-not-re-fired + per-attempt-execute-timeout both HELD; **F-9** (S-3) exponential-backoff `Duration` overflow liveness wedge at large attempt counts ✅ **FIXED** — `exponential` clamps to `max` before any overflowing/negative/wrapped shift or multiply) | — | DST-only (model scope is leasing + crash-recovery, with no time/timeouts, retries/backoff, retry predicates, or retry handlers — see INV-21 "Scope decision" and `formal/tla/README.md` "Scope & limits"; extends INV-8 + INV-9) | `assertRetryTimingAndExhaustionEdges` (per-`(workflowId, stepName)`, scoped to the `retryedge-` id prefix, content-based / F-2-robust: backoff schedule reconstructed from the recorded `RETRYING` timestamps — monotonic in committed time, constant gaps for `fixed` / non-decreasing for `linear`/`exponential`, exactly the strategy/predicate-dictated `RETRYING` count ≤ maxRetries, `retryWhile` strictly fewer than maxRetries+1, and the step resolves to a terminal record) + `assertOnRetryFiredOncePerRetry` (the F-0 analogue: `onRetry` fire count == committed `RETRYING` count — a count `>` is the re-fire break, `<` is a dropped fire); the per-attempt `execute` timeout reaches a terminal `TIMED_OUT` (asserted in the scenario, `assertRetryBound` still bounds attempts) | `RetryTimingAndExhaustionEdgesScenario` · `Inv21RetryTimingAndExhaustionEdgesTest` · seed 0 — **deterministic scenario only; not in the per-step fuzz set (scenario-pinned like INV-9 — the per-attempt `execute` timeout rides the `orTimeout` residual, Phase-3 D5)**; covers backoff timing across crash/replay, the `retryWhile` bound, the `onRetry`-not-re-fired probe across crash/replay, and the per-attempt `execute` timeout → `TIMED_OUT` |
| INV-22 | `EventNameCustomizationSound` | Safety | Yes (customized names applied + stable across replay + routing sound) | — | DST-only (model scope is leasing + crash-recovery, with no event-name customization, an `EventNameCustomizer`, a wire-level event name, a namespace, or status suffixes — see INV-22 "Scope decision" and `formal/tla/README.md` "Scope & limits") | `assertEventNameCustomizationSound` (per-`workflowId`, scoped to the `named-` id prefix, content-based / F-2-robust: every committed step/status event carries the customizer-derived expected `QualifiedName` — custom namespace + `capitalize(stepName)`/`capitalize(workflowBaseName)` base + the status suffix the customizer carries incl. the `stepCompleted`→`Done` override; an issued start produces a present instance that reaches a terminal status; stable across replay jointly with INV-4 — a replayed present step emits nothing, so the recorded customized names are unchanged) | `EventNameCustomizationSoundScenario` · `Inv22EventNameCustomizationSoundTest` · seed 0 (fresh start records the customized names, reaches terminal, crash+replay reproduces identical names, + hand-built pins: good case passes, un-customized namespace throws, default `Completed` suffix where override dictates `Done` throws, dropped start throws, never-terminal throws, out-of-scope prefix skipped); also asserted every step + a horizon pin in `DstSimulation` against a `CustomNamedWorkflow` registered with a custom customizer, folded into the fuzz |
| INV-23 | `EngineSelfProtection` | Safety | Yes (no-corruption contract holds at both surfaces) | **F-5** (candidate — nested primitive surfaces only as a silent deadlock/silent-success under the body-executor threading, never a clean up-front rejection) | DST-only (model scope is leasing + crash-recovery, with no per-instance task queue, body executor, `appendTask` re-entrancy / queue-bound, or nested primitive calls — see INV-23 "Scope decision") | `assertEngineSelfProtection` (per-`workflowId`, scoped to the `selfprot-` prefix, content-based / F-2-robust: the committed history is either well-formed-complete or a clean non-terminal prefix — never torn / duplicate-terminal / orphan; the task-queue overflow throws cleanly without publishing a torn record); `documentNestedPrimitiveNotGuarded` (records the observed outcome — silent-complete vs silent-deadlock, never a clean rejection — the **F-5** candidate, mirroring `documentEffectAtMostOnceGap`) | `EngineSelfProtectionScenario` · `Inv23EngineSelfProtectionTest` · seed 0 — **deterministic scenario only; not in the per-step fuzz set (a nested-primitive instance deliberately stalls non-terminally under a single-threaded executor, the liveness horizon would read as a hang — scenario-pinned like INV-9/16/17/18)**; the nested-primitive observed behaviour under BOTH the default virtual-thread executor (completes cleanly, no corruption) and an injected single-threaded executor (deadlocks → stuck non-terminal, only `STARTED` committed, no corruption — caught by a SHORT wall-clock window), and the task-queue-overflow clean-failure contract driven against the **real** `SimpleWorkflowExecution.appendTask` (its real `ArrayBlockingQueue<>(1000)`), + hand-built pins (a torn/duplicate-terminal/orphan committed log throws; a clean complete or clean prefix passes) |
| INV-24 | `NoWorkAfterRelease` | Safety | Yes for the claim protocol (**F-25/F-31/F-32 FIXED** — the startup scan reads without holding claims, skips peer-owned segments and re-reads on a lost init race); **No for the release window** — S-9 open | **F-25**, **F-31**, **F-32** — FIXED; **S-9** (release-before-drain: `releaseSegment` returns without draining and the framework `Coordinator` releases the claim *before* notifying the engine — model-confirmed double start + double completion, **no failing test**, and NOT the GC-pause zombie deferred to #271); **FW-3** (upstream — `initializeTokenSegments` raises a different exception type per store, which is why the init-race catch is deliberately not narrowed) | `sharding/SegmentClaim.tla` — `NoWorkAfterRelease` + companions `AtMostOneOwner` (segment scope), `ClaimAgreement`, `NoDuplicateExecution`, `BootNeverFails`, `NodeCanJoin`, `NoOrphanSegment`; **VIOLATED** in `SegmentClaim_join_violated_seq` (196 distinct) / `SegmentClaim_restart_violated` / `SegmentClaim_release_violated` / `SegmentClaim_steal_violated`, `No error` in `SegmentClaim_join_fixed_seq` (124 distinct) / `_restart_fixed` / `_release_fixed` / `_steal_fixed` — pairs differ by one boolean | **none — no DST assertion exists and none can today** (the harness has no segment or node dimension; `InMemoryTokenStore` has a no-op `releaseClaim` and no owner column, one JVM shares `ClockUtils.instant()` and the `pid@host` node identity, and `assertAtMostOneOwner` is still called with a literal `1`) | **real infrastructure, not seeds:** `NodeJoinIT` (cold join while a peer holds all four claims; claim rows unchanged when the joiner is ready; peer on a 60 s claim timeout so it cannot be an expired claim), `MultiNodeStartupTest`, `WholeClusterRestartIT`, `ScaleDownAndUpIT`, `RollingDeployIT`, `StoreOutageIT`; plus mocked-`TokenStore` unit tests on the scan |
| INV-25 | `OneInstanceOneOwner` | Safety | Yes (**F-27 FIXED** at **both** call sites) | **F-27 — FIXED** (`removeTerminalAndStartRestoredWorkflowExecutions` took no `Segment`; both predicates were repository-global, so claiming one segment started and evicted executions owned by another — inherited single-segment code, correct while N == 1) | `sharding/WfShard.tla` — `OneInstanceOneOwner`; **VIOLATED** in `cfg_d2_owner` at **317 distinct states** with only `claimSegment` scoped, which is how the **second** call site (`onLiveModeActivated`) was found before the fix was written; `No error` in `cfg_fixed` | **none** — see INV-24's scope note | `WorkflowEngineSegmentClaimStartScopeTest` · `claimingASegmentDoesNotStartTheWorkflowExecutionsOwnedByAnotherSegment` + `aSegmentReachingTheReplayBoundaryStartsOnlyItsOwnWorkflowExecutions` (written as an expected-gap test, observed going **red** when the real fix landed, then inverted); real-infra companions `CrossSegmentSpawnIT`, `ScaleDownAndUpIT` |
| INV-26 | `NoWorkWhileReplaying` | Safety | Yes (**F-28 + F-33 FIXED together**); **residual:** cross-node migration still starts restored bodies at head, blocked upstream on **FW-2** | **F-28** (engine-wide `liveMode`/`currentTrackingToken`) and **F-33** (claim-time restore ignored live mode) — both FIXED, and **must not be split**: fixing F-33 alone while live mode stays engine-wide is a liveness regression; **FW-2** (upstream — claim callbacks omit the token, so the claim-time gate must infer lag rather than read the position) | `sharding/WfShard.tla` — `NoWorkWhileReplaying` + the liveness twin `MigratedInstanceMakesProgress`; `cfg_live_asis` vs `cfg_live_fixed`/`cfg_live_fixed3`, and the decisive arm `cfg_live_trap` → **temporal properties violated, State 17 stuttering, 9,154 distinct states** (the hang the originally-planned fix would have shipped) | **none** — see INV-24's scope note | `WorkflowEngineSegmentLiveModeScopeTest` · `noWorkflowBodyRunsOnASegmentThatHasNotReachedTheStartupLatestToken` + `aRestoredExecutionIsSeededWithTheTokenOfItsOwnSegment` (the lagging segment's token is asserted demonstrably not to cover the boundary, so the arm cannot pass vacuously); `WorkflowEngineClaimDuringReplayTest` · `reclaimingASegmentThatIsStillReplayingDoesNotRunTheRestoredBodyAtHeadState` |
| INV-27 | `TokenNeverPassesUnappliedEvent` | Safety | Yes (**F-26 + F-35 + F-36 FIXED**); does **not** cover F-34b (no wait was ever durably published, so there is no barrier to hold) nor the framework's missing forward clamp (**FW-4**) | **F-26 — FIXED** (one `CheckpointTrigger` and one coalesced pending token served N segments → one segment's request advanced another's stored token past events it never handled: silent event loss on restart); **F-35 — FIXED** (engine shutdown shared a lifecycle phase with the processor, so clearing the repository could win the race against the coordinator drain — a wake lost on every graceful restart); **F-36 — FIXED** (a materialized-but-not-started execution dropped events without evaluating wait conditions **and** reported no pending checkpoint work — two halves, neither fix works alone); **FW-4** (upstream — an over-high request is stored verbatim instead of clamped to `lastConsumedToken`, which is what gave F-26 its blast radius) | `sharding/Holdback.tla` — `TokenNeverPassesUnappliedEvent`, `NoSilentDrop`, `WakeSurvivesHandover`, `NoDuplicateCompletion`; `C1_baseline`, `C2_*`/`C2_short_claim_asworks` **VIOLATED** vs `C3_short_claim_fixed`/`C3b_short_claim_fullfix`/`C3c_holdbackfix_only`, `C4_shutdown_clears_first` **VIOLATED** vs `C4b_shutdown_race_with_fixes` (with `C6_liveness`/`C7_liveness_shutdownrace`), `C5_partial_drain`, `C8_two_instances_one_segment`; `M1_no_holdback`/`M2_single_round_barrier`/`M3_restore_ignores_completion` are the mutation arms proving the green sides non-vacuous | **none** — see INV-24's scope note | `WorkflowEngineCrossSegmentCheckpointTest` · `asyncCheckpointOfOneSegmentLeavesTheStoredTokenOfAnotherSegmentAtItsOwnPosition` (parameterized over fully-deferred and auto checkpointing so the mode cannot be blamed; one real framework `CheckpointingProgressStrategy` per segment, assertion read back from a real token store) + `aCheckpointRequestReachesOnlyTheTriggerOfItsOwnSegment` + `releasingOneSegmentKeepsTheOtherSegmentsCheckpointing`; `WorkflowConfigurationDefaultsTest` · `workflowEngineShutsDownAfterTheEventProcessorHasDrained`; `WorkflowEngineReplayTest` for F-36's queued-match half; real-infra `DurableWaitAcrossRebalanceIT` (5 kill runs + 3 graceful runs × 3 rounds + 1 control, ~1000 instance-level observations, **zero losses**) |
| INV-28 | `SpawnExactlyOnce` | Safety | Yes — the campaign's strongest **precise negative**, no finding | — (double starts trace to ownership, not routing: INV-25/F-27 fixed, S-9 open. Untested adjacent hazard: **S-10**, `#` is silently load-bearing in the segment key, so a `workflowIdProvider` returning `order#123` collapses every order onto one segment) | `sharding/WfShard.tla` — `SpawnExactlyOnce` + `WakeExactlyOnce`, with `NoDoubleStart` and `AtMostOneCompletion`; both hold with **all four modelled defects enabled** at 3 segments / 3 instances / **1,722,131 distinct states, depth 31** (`cfg_asis3_spawn`, `cfg_asis3_wake`, `cfg_fixed3`), and both fail within **8 steps** once the `shouldHandle`/`shouldSpawn` guards are deleted (`cfg_mut`, `cfg_mut_wake`) — so the green is not vacuous | **none** — see INV-24's scope note | **real infrastructure, not seeds:** `DuplicateStartEventsIT` (8 duplicate starts + 8 duplicate resumes committed, effect once, on **both** the hash-routed and the broadcast path); `TimerAcrossHandoverIT` (timers **RESUME** at the original deadline across a handover — a 60 000 ms timeout fired at `msFromArmed=60110` on the node that *took* the segment, retry backoff at `msFromFirstAttempt=30018`; deadlines come from the durable step timestamp); `IdleClusterSparseTrafficIT` (claim renewal is **not** event-driven — `maxClaimAgeMs=6014` against a 10 s timeout across a 120 s idle gap, owners unchanged, and placement after idleness still routes by hash rather than to the warm node); `CrossSegmentSpawnIT`; `SustainedLoadHandoverIT` |
