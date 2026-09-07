# axon-flow-spec — Architecture Reconnaissance (Phase 0)

Reconnaissance for hardening the task-leasing / crash-recovery protocol via TLA+ (design) and
deterministic simulation testing (implementation). **Docs only; no code was changed.** Every claim
cites `relative/path:LINE` against the tree at branch `poc/tla_dst`.

Terminology note used throughout: the engine has TWO distinct token concepts that must not be
confused. (1) The **processor tracking token** — Axon's pooled-streaming-processor read position,
stored in `TokenStore[Workflow]`, which here is **in-memory and non-durable** (see §2/§4). (2) The
**engine safe-point token** — a separate, durable lower-bound-of-restart-tokens, stored in
`SafePointStore` (durable in production). Crash recovery rides on (2) + replay, not on (1).

---

## 1. Overview & layer map

An event-sourced durable-execution workflow engine on Axon Framework 5. Workflows are long-running
imperative bodies; the runtime persists every state change as an event and, on restart, replays the
event stream to rebuild each instance's state, then resumes live execution from the first un-recorded
step. Workflow bodies run on a single-threaded-per-instance task queue.

**Versions (cited):**
- **Axon Framework: `5.1.1`** — `pom.xml:40` (`<axon.version>5.1.1</axon.version>`); modules
  `axon-messaging`, `axon-eventsourcing`, `axon-common` (`pom.xml:84-148`).
- **Java: `21`** — `pom.xml:35-36` (`maven.compiler.source/target = 21`); CI also runs JDK 25
  (`.github/workflows/main.yml`, `pullrequest.yml` matrix).
- **Kotlin: `2.3.21`** — `pom.xml:38`. **JUnit `6.1.0`** — `pom.xml:41`. **ArchUnit `1.4.2`** —
  `pom.xml:39`.

**Maven reactor modules** (`pom.xml:60-65`): `dsl`, `spring-boot`, `runtime`, `test`; plus `examples`
via the default-active `examples` profile (`pom.xml:328-339`, active unless `-DskipExamples`).

| Layer | Path | Role |
|---|---|---|
| runtime | `runtime/src/main/java/io/axoniq/framework/workflow/runtime/` | Command-mode primitives, execution state, event sourcing, engine, config |
| runtime API (definitions) | `runtime/.../api/execution/context/` | Immutable record DTOs (`ExecuteStepDefinition`, `Timing`, `RetryPolicy`, …) + `WorkflowExecution`/`WorkflowContext` interfaces |
| config (Axon wiring) | `runtime/src/main/java/io/axoniq/framework/workflow/configuration/` | Event-processor/token-store/engine registration enhancers |
| dsl | `dsl/src/main/java` + `.../kotlin` | Java (`BaseWorkflowContext`/`SimpleWorkflowContext`) and Kotlin (`WorkflowKontext`/`Kontext`) author-facing DSLs |
| spring-boot | `spring-boot/src/main/java/io/axoniq/framework/workflow/springboot/` | Auto-configuration, `@Workflow` bean detection/grouping |
| examples/simple | `examples/simple/` | In-memory integration suite (no Docker) |
| examples/bike-rental | `examples/bike-rental/` | Spring Boot + Testcontainers (Axon Server) integration |
| test | `test/src/main/java/io/axoniq/framework/workflow/runtime/test/` | `AbstractDeclarativeTestBase`, recording event store, `DelayedPublisher` |

The engine is an Axon `EventHandler` + `ReplayStatusChangedHandler`
(`WorkflowEngine.java:59`) driven by one **pooled-streaming event processor**.

---

## 2. Task leasing / ownership

**Who owns processing a workflow instance** is decided at the granularity of the **whole engine /
event-processor**, not per workflow instance. There is no per-instance lease. Ownership =
"this process holds the claim on segment 0 of the workflow event processor."

- The engine runs inside one **`EventProcessorModule.pooledStreaming(moduleName)`** with a SINGLE
  segment covering all events: `WorkflowEventProcessingRegistrationEnhancer.java:102-105` builds the
  module; `.customized(ANY_EVENT_IN_ONE_SEGMENT)` (`:157`) pins it. `ANY_EVENT_IN_ONE_SEGMENT`
  (`AllEventEventHandlingComponent.java:58-73`) sets `.initialSegmentCount(1)` (`:72`) and
  `.batchSize(1)` (`:73`, comment: "only 1 is supported / working").
- **Claim/lease lifecycle = Axon pooled-streaming-processor segment claiming via a `TokenStore`.**
  The processor claims segment 0's token to read events; Axon's PSEP machinery owns acquire / hold /
  heartbeat / expiry / release. This engine does not implement leasing itself — it delegates to
  Axon's processor.
- **CRITICAL — the processor's own TokenStore is hardcoded in-memory:**
  `AllEventEventHandlingComponent.java:70` → `.tokenStore(new InMemoryTokenStore())`. So the
  *processor tracking token* (read position / lease) is **per-process and non-durable**. There is no
  cross-node claim hand-off through this token; in a multi-node deployment each node's processor has
  its own in-memory token store, so genuine single-writer leasing across nodes is **NOT** provided by
  this component. (`UNCLEAR:` whether production deployments are expected to be single-node, or
  whether something outside this repo overrides `TokenStore[Workflow]`. The Spring autoconfig
  (`WorkflowAutoConfiguration.java`) does not register a durable one for the processor — bike-rental
  only overrides the *safe-point* token store, `BikeRentalApplication.java:56-70`.)
- The durable component is the **`SafePointStore`** (§5), registered under
  `COMPONENT_SAFE_POINT_STORE` (`WorkflowConfigurationDefaults.java:73,161-179`). It is backed by a
  separate `TokenStore` under identifier `<module>SafePoint`
  (`TokenStoreSafePointStore.java:64-66`) so it never collides with the processor's token. Its own
  acquire/store/release uses `fetchToken`/`storeToken`/`releaseClaim` on segment 0
  (`TokenStoreSafePointStore.java:69-107`).
- **Per-instance routing/ownership** (within the single owning process) is by `workflowId`: the
  engine looks up the instance in `WorkflowExecutionRepository.findById(workflowId)`
  (`WorkflowEngine.java:104`) and only handles events whose `workflowId` it owns; events for unknown
  ids are skipped (`:105-109`, "expected in multi-module setups").

**Net:** "leasing" here is Axon PSEP single-segment token claiming, but the token store used for it
is in-memory (`AllEventEventHandlingComponent.java:70`). Durability/recovery is bolted on via the
separate `SafePointStore` + full replay, not via a durable processor lease.

---

## 3. Dispatch / step execution

Workflow bodies call command-mode primitives (`execute`, `waitForEvent`, `modifyPayload`, `version`,
`terminate`); each returns a `WorkflowStepResult` (ADR-001, `specs/workflow/adrs/001-...md`).
Primitives are implemented by delegates wired in `WorkflowContextDelegation`
(`WorkflowContextDelegation.java:116-160`): `ExecuteDelegate` wrapped by `RetryableExecuteDelegate`,
`WaitForDelegate`, `TerminateDelegate`, `PayloadDelegate`, `VersionDelegate`. All extend
`AbstractStepExecutor`.

**Single-threaded task queue (the core invariant carrier):**
- Each `SimpleWorkflowExecution` owns one `BlockingQueue<Consumer<WorkflowExecution>> taskQueue =
  new ArrayBlockingQueue<>(1000)` (`SimpleWorkflowExecution.java:92`, capacity FIXME).
- The workflow body runs on ONE driver thread (`execute(...)` →
  `executeWithResultInSeparateThread`, `SimpleWorkflowExecution.java:142-169`). Every state mutation
  is enqueued as a task via `appendTask` (`:472-477`; throws "Too many tasks" if full — no
  backpressure, FIXME `:475`) and applied by `awaitStateChange`, which loops
  `taskQueue.take(); task.accept(this)` until a predicate holds (`:346-353`).
- **"No nested primitives" enforcement** is `AbstractStepExecutor.acceptAllPendingTasksForStep`
  (`AbstractStepExecutor.java:102-110`): before a primitive registers its own work it drains
  pending tasks for the current step. Because there is exactly one driver thread per instance and the
  queue is single-consumer, a primitive cannot run while another is mid-flight. Each delegate calls
  `acceptAllPendingTasksForStep(stepName)` first (`ExecuteDelegate.java:115`,
  `WaitForDelegate.java:91`).
- Event publication is offloaded to the executor via
  `ProcessingContextUtils.executeWithResult` (`AbstractStepExecutor.java:213-225`) which creates a
  child UoW with `workScheduler(executor)` and publishes through `EventSink.publish`
  (`:222`). The driver thread blocks on the result handle, but the publish itself runs on the
  executor; the resulting event is then routed back as a task that calls `state().evolve(...)`.

**Result handles:** `stateBased(stepName, …)` returns a `StateBasedWorkflowStepResult` whose
blocking `await()` drives `awaitStateChange(s -> true)` (`AbstractStepExecutor.java:254-258`).

---

## 4. Persisted event log & storage backend

**What is persisted:** one event per workflow lifecycle/step transition. Step events carry step
status in metadata; the catalog of statuses applied on replay is in
`EventSourcedWorkflowState.evolve` (`EventSourcedWorkflowState.java:200-248`): step
`STARTED/COMPLETED/FAILED/TIMED_OUT/CANCELLED/RETRYING`, and workflow-level
`STARTED/COMPLETED/FAILED/CANCELLED/TIMED_OUT` (`:251-281`). Payload changes ride on `COMPLETED`
events via a named payload reducer (`:226-233`, `evolvePayload` `:307-335`). Migration steps are
`COMPLETED` events carrying `versionChangeId`+`version` metadata (`:189-191,287-299`). The workflow's
semver travels on `MessageType.version()` of every emitted event (ADR-005 §2;
`EventSourcedWorkflowState.java:274-278`).

**Where:**
- **Event store** = Axon `EventStore` / `StreamableEventSource` (`AllEventEventHandlingComponent.java:69`;
  `WorkflowEventProcessingRegistrationEnhancer.java:168-169`). The engine reads via the processor and
  writes via `EventSink.publish` (`AbstractStepExecutor.java:222`,
  `WorkflowContextDelegation.java:271`).
- **Processor token store** (read position / lease) = `new InMemoryTokenStore()`, **hardcoded**
  (`AllEventEventHandlingComponent.java:70`) — non-durable (see §2).
- **Engine safe-point store** (durable recovery anchor) = `SafePointStore`. In-memory default
  (`InMemorySafePointStore.java`) or `TokenStoreSafePointStore` when a `TokenStore` named
  `COMPONENT_SAFE_POINT_TOKEN_STORE` is present (`WorkflowConfigurationDefaults.java:161-178`).
- **Workflow-instance repository** = `InMemoryWorkflowExecutionRepository`
  (`WorkflowConfigurationDefaults.java:188-192`), a `ConcurrentHashMap<String, WorkflowExecution>`
  (`InMemoryWorkflowExecutionRepository.java:44`). **Always in-memory** — live executions exist only
  for the current process; they are reconstructed by replay on restart.
- **History/read-model** = `InMemoryWorkflowHistoryRepository` (`WorkflowConfigurationDefaults.java:194-198`),
  fed by `WorkflowHistoryProjector` (a second event-handling component on the same processor,
  `WorkflowEventProcessingRegistrationEnhancer.java:128-153`).

**In-memory vs production:**
| Component | In-memory (default / examples/simple) | Production (bike-rental) |
|---|---|---|
| EventStore | `InMemoryEventStorageEngine` (`WorkflowReplayPreparedStateTest.java:164`) | Axon Server DCB (`BikeRentalIT.kt:48-51`) |
| Processor TokenStore | `InMemoryTokenStore` (hardcoded `AllEventEventHandlingComponent.java:70`) | **still** `InMemoryTokenStore` (hardcoded) |
| SafePoint TokenStore | `InMemorySafePointStore` | `JdbcTokenStore`, schema `WF_TOKEN_ENTRY` (`BikeRentalApplication.java:56-70`, `WorkflowConfigurationDefaults.java:82-92`) |
| Default projection TokenStore | n/a | `JpaTokenStore` `@Primary` (`BikeRentalApplication.java:79-89`) |
| Execution repo | `InMemoryWorkflowExecutionRepository` | same (in-memory) |

---

## 5. Replay / recovery

ADR-004 (`specs/workflow/adrs/004-workflow-replay-safe-point-tokens.md`) is implemented exactly as
follows. **On startup / crash-restart:**

1. **Reset-token selection** — `WorkflowEventProcessingRegistrationEnhancer` registers a
   `<module>ReplayResetHook` at phase `INBOUND_EVENT_CONNECTORS - 10`
   (`WorkflowEventProcessingRegistrationEnhancer.java:57,158-193`). It ensures the processor token
   store has a segment (`ensureSegmentsInitialized`, `:198-217`), then `resetOrSwitchToLiveMode`
   (`:219-236`): `determineResetToken` fetches the stored safe-point token; if none, falls back to
   `eventSource.firstToken()` (`:238-249`).
2. **Decide replay vs live** — `requiresReplay(resetToken, latestToken)` = "not at same position as
   latest" (`:252-257`). If replay needed: `workflowEngine.initializeSafePoint(resetToken)` then
   `processor.resetTokens(resetToken)` (`:233-234`), which makes Axon wrap the token in a
   **`ReplayToken`** and re-stream from `resetToken`. Otherwise it initializes the safe point to
   `latestToken` and switches straight to live mode (`:229-231`).
3. **Replay (rebuild state, no side effects)** — during replay each event hits
   `WorkflowEngine.handle(EventMessage,…)` (`:88-127`). Brand-new starts are created from start
   events (`checkAndCreateNewWorkflow`, `:204-256`); subsequent events route to the instance's
   `onEvent`. In replay mode (`executable == false`) `onEvent` applies state **synchronously**:
   `state().evolve(eventMessage, processingContext)` (`SimpleWorkflowExecution.java:362-365`). No
   tasks, no executor — pure state reconstruction.
4. **Switch to live** — when Axon reports replay finished,
   `WorkflowEngine.handle(ReplayStatusChanged,…)` (`:132-145`) calls `switchToLiveMode` (`:152-180`):
   removes finished executions, persists the safe point, then `execute(...)` each surviving execution
   (`:172-174`).
5. **Resume from first un-cached step** — `execute` runs the body again
   (`SimpleWorkflowExecution.java:138-169` → `executeWorkflow`, `:197-226`). Each primitive checks
   `state().containsStep(stepName)`; if present, the recorded `WorkflowStep` is returned as a
   **cached result** and no new event is emitted (e.g. `ExecuteDelegate.java:117` — the body that
   emits `started` runs ONLY when the step is absent; otherwise it proceeds to read the existing step
   at `:131`). Workflow `STARTED` is re-emitted only if status is still `NONE`
   (`SimpleWorkflowExecution.java:178-189`).

**Engine safe-point token = lower bound of active restart tokens:**
- Each `SimpleWorkflowExecution` captures an immutable `restartToken` at construction from the start
  event's processing context (`SimpleWorkflowExecution.java:120` →
  `ProcessingContextUtils.resolveRestartToken`, `:151-156`; the engine seeds
  `RESTART_TOKEN_RESOURCE_KEY` per event in `WorkflowEngine.java:94-95`).
- `determineEngineSafePoint` (`WorkflowEngine.java:317-331`): empty repo → current tracking token;
  else `earliestToken = lowerBound` over every execution's `restartToken()` (`:328`); if any
  execution lacks a restart token it returns `null` (skips persist). Persisted via
  `persistEngineSafePoint` after create/remove/shutdown/switch
  (`:164,195,249,284,310-315`).
- Tokens are **unwrapped** before storing: `captureCurrentTrackingToken` calls
  `WrappedToken.unwrapLowerBound(token)` (`:298-308`) to avoid mixing raw and `ReplayToken` types
  (comment `:302-304`: otherwise `determineEngineSafePoint` crashes with "Incompatible token type
  provided: ReplayToken").

**Drift guard (ADR-004 + ADR-005 §5):**
- Per-invocation `Set<String> referencedStepNames` (`SimpleWorkflowExecution.java:95`), cleared at
  the start of every body run (`:205`). Each event-emitting primitive calls `recordStepReference`
  before publishing (`ExecuteDelegate.java:113`, `WaitForDelegate.java:89`, `PayloadDelegate.java:78`,
  `VersionDelegate.java:100`, `TerminateDelegate.java:127`).
- `guardAgainstReplayDrift(name)` (`WorkflowExecution.java:282-287`) throws
  `WorkflowReplayDriftException` if `hasUnreferencedTerminalStep()` — i.e. state contains terminal
  steps the current run has not referenced (`unreferencedTerminalSteps`, `:254-260`). Called before
  first live publish of an absent step (`ExecuteDelegate.java:118`, `WaitForDelegate.java:94`,
  `PayloadDelegate.java:81`) and before workflow-level termination
  (`TerminateDelegate.java:101` with marker `"<terminate>"`; `:128` for step cancel).
- **Non-terminal handling:** `WorkflowReplayDriftException` is caught in
  `SimpleWorkflowExecution.handleWorkflowException` (`:290-298`) — logs a warning and publishes
  **no** terminal event, leaving the instance in its current state so a corrected redeploy can replay
  cleanly (`WorkflowReplayDriftException.java:26-40`).

---

## 6. Retries

`RetryPolicy` (`runtime/.../retry/RetryPolicy.java`): `maxRetries(n)` = n retries beyond the first
attempt → **n+1 total executions** (`RetryPolicy.java:28-33,76-78`); `shouldRetry` =
`attempt <= maxRetries && retryPredicate.test(...)` (`:119-121`). `RetryPolicy.NONE` and
`maxRetries<=0` short-circuit to the plain delegate (`RetryableExecuteDelegate.java:90-92`).

**Backoff** (`BackoffStrategy.java`): `NONE` (zero, `:35`), `fixed` (`:51`), `linear`
(`baseDelay*attempt`, `:61`), `exponential` (`min(base*2^(attempt-1), max)`, `:72-78`).

**Per-attempt timeout:** default **5s**, set in the DSL: `BaseWorkflowContext.defaultTimeout =
Duration.ofSeconds(5)` (`dsl/.../BaseWorkflowContext.java:75`), wrapped into `new Timing(...)`
(`:516,538`). Applied in `ExecuteDelegate`: `remainingTimeout = between(now, startTime + timeout)`
then `result.orTimeout(remainingTimeout, MS)` (`ExecuteDelegate.java:134-135,167-168`). Timeout is
measured from the recorded step `STARTED` timestamp (`step.timestamp()`, `:133`), so it survives
replay.

**Where applied / orchestration** (`RetryableExecuteDelegate`):
- Normal first attempt `launchWithRetry(command, 1)` (`:107,112-120`) installs custom failure/timeout
  handlers that call `handleAttemptFailure(...)` (`:123-167`).
- On retryable failure: emit a `RETRYING` step carrying `StepRetryInfo(attempt, maxRetries, error)`
  (`:138-139`), wait for it to be recorded (`:141-149`), compute `retryReadyAt` from backoff
  (`:158`), then `scheduleRetryAttempt` (`:182-213`).
- Delayed retry uses `CompletableFuture.delayedExecutor(delay, MS)` (`:200-206`), registered as a
  running step so it is cancellable.
- **Crash recovery of retries:** if the instance comes back with a persisted `RETRYING` step,
  `execute` recomputes `retryReadyAt` from `info.attempt()` + recorded `step.timestamp()` and
  reschedules (`:96-104,172-178`). So retry timing is reconstructed from the event log, not from
  in-memory state.

---

## 7. Idempotency / dedup

**Step-name strings are the durable dedup keys.** State is keyed by step name in a `Map<String,
WorkflowStep>` (`EventSourcedWorkflowState.java:62`).

- **At-most-once *recording* (replay-idempotent applies):** `evolve` ignores any step transition once
  the step is already terminal (`EventSourcedWorkflowState.java:195-199`) and any workflow transition
  once the workflow is terminal (`:253-257`). Migration steps use `versions.putIfAbsent` —
  first-writer-wins (`:293`, ADR-005 "first-writer wins per changeId").
- **Publish-side guards:** `sendStepEvent` refuses to publish if the workflow is terminal
  (`AbstractStepExecutor.java:197-203`) or the step is already terminal (`:204-212`). Many `appendTask`
  callbacks re-check `!status().isTerminal()` before emitting (`ExecuteDelegate.java:194`,
  `WaitForDelegate.java:115,131,150`, `RetryableExecuteDelegate.java:194,202`).
- **"Already in state → cached result":** primitives only emit a `STARTED` event when
  `!state().containsStep(stepName)` (`ExecuteDelegate.java:117`, `WaitForDelegate.java:93`); on replay
  the step exists, so the recorded result is returned and the side-effecting action is **not**
  re-run. This is the dedup that makes replay safe.

**Guarantee characterization:**
- **At-most-once *recording*** of a given `(stepName, terminal-status)` is enforced by the terminal
  guards above — a step's terminal outcome is written once.
- **At-least-once *execution* of side effects** is the realistic guarantee: the `action.apply(...)`
  inside `execute` (`ExecuteDelegate.java:148`) runs whenever the step is in `STARTED`/`RETRYING` and
  not yet completed. If the process crashes **after** the action ran (side effect happened) but
  **before** the `COMPLETED` event committed, the step is still `STARTED` on replay and the action
  runs again. There is no transactional outbox tying the external side effect to the event commit.
  **RISK for TLA+/DST:** see §13.
- The code itself flags this gap: `ExecuteDelegate.java:130` `// FIXME -> consider to use QOS (at
  least once/at most once)` and repeated `// FIXME - This is where we should publish using an append
  condition` (`:163,173,178,183,188`) — i.e. optimistic-concurrency append conditions are **not yet**
  used, so two writers could both append (relevant only if leasing is broken, see §2).

---

## 8. Concurrency model

- **One pooled-streaming event processor, one segment, batchSize=1** (`AllEventEventHandlingComponent.java:72-73`).
  So events are delivered to the engine effectively serially by one Axon WorkPackage thread.
- **Engine executor:** `WorkflowEngineExecutor` = `Executors.newVirtualThreadPerTaskExecutor()`
  (`WorkflowConfigurationDefaults.java:142-146`), resolved per-instance in
  `WorkflowContextDelegation` (`:109-111`). Used to offload event publication and to run each workflow
  body on its own thread.
- **Per-instance single-threaded body:** each `SimpleWorkflowExecution` runs its body on one virtual
  thread (`executeWithResultInSeparateThread`, `ProcessingContextUtils.java:113-125`) and serializes
  ALL state changes through its single-consumer `ArrayBlockingQueue` task queue
  (`SimpleWorkflowExecution.java:92,346-353`). This is the central ordering assumption: within one
  instance, state evolves in task-queue order, never concurrently.
- **Async side channels feeding the queue:** timeouts/backoff use
  `CompletableFuture.delayedExecutor(...)` (`WaitForDelegate.java:138`,
  `RetryableExecuteDelegate.java:206`) on the common ForkJoinPool; awaited events arrive on the
  processor thread (`WorkflowEngine.handle` → `execution.onEvent` → `appendTask`,
  `SimpleWorkflowExecution.java:356-366`). These all converge by **enqueuing tasks**, so they don't
  mutate state directly — the queue is the serialization point. (Exception: `runningSteps` futures
  complete on arbitrary threads but only call `appendTask`/`removeRunningStep`.)
- **Live vs replay branch:** `onEvent` mutates state directly during replay (single-threaded
  reconstruction) but enqueues during live (`SimpleWorkflowExecution.java:356-365`).
- **Shutdown:** `WorkflowEngine.shutdown` interrupts each instance (`:277-286`); `interrupt()` offers
  a poison task that sets the driver thread's interrupt flag (`SimpleWorkflowExecution.java:451-458`).
- **Cross-instance:** distinct instances run on distinct virtual threads with independent queues;
  the only shared mutable structure is the `ConcurrentHashMap` execution repo
  (`InMemoryWorkflowExecutionRepository.java:44`).

**Ordering caveats for determinism:** `EventWaitConditions.evaluateAndApply` iterates a
`ConcurrentHashMap.entrySet()` (`EventWaitConditions.java:111`) — if one event satisfies multiple
wait conditions, completion order is hash-order, not deterministic. `workflowStepNames()` sorts by
`WorkflowStep::timestamp` (`EventSourcedWorkflowState.java:166-172`) — deterministic only if
timestamps are unique (they come from the injected clock; ties → unspecified order). `findAll()`
returns an unordered `Set.copyOf(...)` (`InMemoryWorkflowExecutionRepository.java:62-63`), iterated in
`switchToLiveMode` (`WorkflowEngine.java:172`) and `determineEngineSafePoint` (`:323`).

---

## 9. Existing tests

**Harness — `AbstractDeclarativeTestBase<T extends WorkflowContext>`**
(`test/src/main/java/io/axoniq/framework/workflow/runtime/test/AbstractDeclarativeTestBase.java`):
- `@BeforeEach setUp` (`:69-82`): builds a `WorkflowConfigurer.create()`, registers a `WorkflowModule`
  built from `getDeclaredDefinition()` (+ optional `getAdditionalDefinitions()` for multi-version,
  `:102-115`), applies an overridable `configure()` hook (`:89-91`), and `.start()`s the Axon config.
  Exposes `workflowEngine`, `workflowRegistry`, `delayedPublisher`, `workflowHistoryRepository`.
- `@AfterEach shutdown` (`:117-126`): describes components, `workflowEngine.shutdown()`,
  `configuration.shutdown()`, clears history.
- Backing store wiring comes from `WorkflowConfigurer` defaults (in-memory event store, in-memory
  token store) plus `WorkflowTestEnhancer` which decorates the `EventStore` with a
  `PrettyPrintingRecordingEventStore` for assertions
  (`test/.../configuration/WorkflowTestEnhancer.java:52-55`).

**`examples/simple` structure:** workflows under `src/main/.../workflow/` (Java) and
`src/main/kotlin/...`; tests under `src/test/...` named `*DeclarativeTest` / `*IntegrationTest`. A
test typically nests `DeclarativeTest`/`AutodetectedTest` subclasses of `AbstractDeclarativeTestBase`
and drives events through `DelayedPublisher`, asserting with Awaitility
(`UserSignupTest.java:58-196`). Crash/replay is simulated by pre-seeding an `InMemoryEventStorageEngine`
+ `SafePointStore` and starting a fresh app
(`WorkflowReplayPreparedStateTest.java:162-268` — the closest existing analogue to DST). Notable tests:
`RetryWorkflowDeclarativeTest`, `StepTimeoutWorkflowDeclarativeTest`, `AwaitEventTimeoutDeclarativeTest`,
`MultiVersionRoutingDeclarativeTest`, `ParallelWorkflowsDeclarativeTest`,
`WorkflowTerminationCleanupTest`, `WorkflowReplayPreparedStateTest`. Kotlin parity tests under
`examples/simple/src/test/kotlin/...`.

**How to run (matches CI conventions):**
- Full reactor incl. examples: `./mvnw -B -ntp clean verify` (examples active by default).
- Runtime + DSL only (no examples): add `-DskipExamples` or narrow with `-pl runtime,dsl -am`. Per
  `AGENTS.md`, when runtime/DSL behavior changes, **include `examples/simple`** explicitly in the
  reactor.
- Integration tests (failsafe) run under `-Pintegration-test` (CI uses
  `-Pintegration-test -Djacoco.skip=true`); failsafe binds at `integration-test` phase
  (`pom.xml:179-182`).

**Docker:** only **`examples/bike-rental`** needs Docker. `BikeRentalIT.kt` is `@Testcontainers` with
an `AxonServerContainer` (`BikeRentalIT.kt:43-51`) and the app uses `spring-boot-docker-compose`
(`examples/bike-rental/pom.xml:106`, `docker-compose.yaml:19` → `axonserver:2025.2.7`).
`examples/simple` and the `runtime`/`dsl` unit suites are pure in-memory (no Docker).

---

## 10. Existing CI

Workflows in `.github/workflows/`: `main.yml`, `pullrequest.yml`, `docs.yml`,
`release-notes.yml`, `dependabot-automation.yml`.

| | `main.yml` | `pullrequest.yml` |
|---|---|---|
| Trigger | `push` to `main`, `paths-ignore: docs/**` | `pull_request`, `paths-ignore: docs/**` |
| Concurrency | — | per-PR, cancel-in-progress |
| Runner / timeout | ubuntu-latest, 25 min | same |
| JDK matrix | 21 (sonar+deploy), 25 | 21 (sonar), 25 |
| Build (non-sonar) | `./mvnw -B -U -ntp -Possrh -Pintegration-test -Djacoco.skip=true -Dsurefire.rerunFailingTestsCount=5 clean verify` | same |
| Build (sonar) | `./mvnw … -Dcoverage -Dsurefire.rerunFailingTestsCount=5 clean verify` | `… -Possrh -Dcoverage …` |
| Extra | Sonar; deploy to Sonatype on JDK21; Slack (disabled) | Sonar (same-repo PRs only); `test-summary/action@v2` |

**For a new DST job:** match conventions — `ubuntu-latest`, `setup-java@v5.2.0` (zulu), `cache: maven`,
`./mvnw -B -U -ntp …`, JDK 21 primary. Flaky-rerun via `-Dsurefire.rerunFailingTestsCount=5` is used
repo-wide (DST should run with a fixed seed and **not** rely on reruns to pass). Docker-backed
bike-rental runs in the same job today (no separate Docker job), so a DST job that stays in-memory
needs no Docker.

---

## 11. Sources of nondeterminism (exhaustive)

"In replay path?" = does it execute while the engine rebuilds state during a restart/replay (replay
mode applies events synchronously via `evolve`, §5)? Most timing/scheduling lives in the **live**
execution path, not replay, because replay only reconstructs state.

| Source | Location (file:line) | In replay path? | Already injectable? | Notes |
|---|---|---|---|---|
| Wall clock `clock.instant()` | `WaitForDelegate.java:97,110,117`; `ExecuteDelegate.java:94,134`; `RetryableExecuteDelegate.java:158,162,189`; `SimpleWorkflowExecution.java:279` | Indirectly — recorded timestamps drive replay timeout math via `step.timestamp()` | **Yes** — `Clock` injected (constructor param on every delegate; component `WorkflowConfigurationDefaults.java:135-140`) | Default `Clock` = `GenericEventMessage.clock` (deprecated; AF issue #3083). Timeouts computed from recorded `STARTED` timestamp survive replay. |
| `Instant.now(clock)` | `AbstractStepExecutor.java:182`; `ExecuteDelegate.java:134`; `RetryableExecuteDelegate.java:189` | Indirectly | **Yes** — uses injected `clock` | Same seam as above. |
| Event/step timestamps | `EventSourcedWorkflowState.java:204,217,223,229,236,243` use `eventMessage.timestamp()` | **Yes** — read during `evolve` | Indirect — set by clock at publish time | Replay determinism depends on persisted timestamps; ties affect `workflowStepNames()` sort (`:166-172`). |
| RNG (`Random`/`Math.random`) | — none in `runtime/` | n/a | n/a | No RNG in runtime/dsl. Only randomness is UUID below. |
| UUID generation | `ProcessingContextUtils.java:79` (`UUID.randomUUID()` for UoW id); `SimpleWorkflowExecution.java:509` (workflow-event UoW id) | No (live publish/UoW only) | **No** — hardcoded `UUID.randomUUID()` | UoW ids are not persisted as workflow state, but feed logs/UoW identity; for DST determinism a seedable id source is desirable. **workflowId** itself comes from `WorkflowIdProvider` over event payload (deterministic), not UUID. |
| Per-attempt / step timeout scheduling | `ExecuteDelegate.java:167-168` (`orTimeout`); `WaitForDelegate.java:126-139`, `RetryableExecuteDelegate.java:200-206` (`delayedExecutor`) | No (live) | Partial — duration derived from injected `clock`, but the **scheduler** (`CompletableFuture.delayedExecutor` → ForkJoinPool / internal `Delayer`) is **not** injectable | Real timer threads; nondeterministic firing order vs task queue. Major DST seam. |
| Thread / executor scheduling | `WorkflowConfigurationDefaults.java:142-146` (virtual-thread-per-task); `ProcessingContextUtils.java:113-125`; `AbstractStepExecutor.java:99` | No (replay is single-threaded synchronous) | **Yes for the body executor** — `ExecutorService` is a registered component (`WORKFLOW_ENGINE_EXECUTOR`), replaceable via `registerIfNotPresent`. **No** for ForkJoinPool used by `runAsync`/`delayedExecutor` | Replacing the engine executor with a deterministic/same-thread executor is the main concurrency seam; the `delayedExecutor` ForkJoinPool path is a separate, harder seam. |
| `ArrayBlockingQueue.poll()/take()` ordering | `SimpleWorkflowExecution.java:350,468` | No (live) | n/a (FIFO, deterministic given enqueue order) | FIFO is deterministic; nondeterminism enters via *who enqueues when* (timers/events on other threads). |
| `HashMap`/`HashSet`/`keySet`/`entrySet` iteration | `EventWaitConditions.java:111` (entrySet, match dispatch); `RunningSteps.java:99,101`; `EventSourcedWorkflowState.java:167`; `InMemoryWorkflowExecutionRepository.java:62-63` (`Set.copyOf`) | `evolve` itself doesn't iterate maps; `findAll()` iterated in `switchToLiveMode` (`WorkflowEngine.java:172`) **is** on the restart path | No (intrinsic) | Wait-condition dispatch order across multiple matches is hash-order. `findAll()` Set is unordered → order of restoring/executing multiple instances is nondeterministic. `lowerBound` over a Set (`:323-328`) is order-independent (commutative), so safe-point value is deterministic. |
| External I/O (step `action`) | `ExecuteDelegate.java:148` (`action.apply(procContext, payload)`) | No (cached on replay; only runs for non-recorded steps) | Yes — user-supplied; in tests replaced by fixtures (`examples/simple/.../fixture/`) | Side effects are at-least-once (§7). DST should model these as mockable effects. |
| Event-arrival ordering / interleaving | `WorkflowEngine.handle` (`:88-127`); `onEvent` (`SimpleWorkflowExecution.java:356-366`) | Replay = deterministic stream order; **live = nondeterministic** external arrival | Controllable in tests via `DelayedPublisher` / direct `eventSink.publish` | The key environmental nondeterminism the TLA+ model must capture: crash points + external event interleaving relative to step completion. |
| Token unwrap / safe-point compute | `WorkflowEngine.java:305,317-331` | Yes (restart) | n/a | `lowerBound` is commutative; deterministic given the set of restart tokens. |
| Replay vs live mode flip timing | `WorkflowEngine.handle(ReplayStatusChanged)` (`:132-145`); `isRunning` `AtomicBoolean` (`:66,153`) | Boundary | n/a | Race: events arriving exactly at the live-switch boundary; `switchToLiveMode` is guarded by `getAndSet` but `checkAndCreateNewWorkflow` reads `isRunning.get()` (`:250`). |

---

## 12. Phase 3 seam candidates

| Seam | Where constructed / hardcoded | Existing injection point? |
|---|---|---|
| **Clock** | Default `GenericEventMessage.clock` registered `WorkflowConfigurationDefaults.java:135-140`; threaded into every delegate constructor (`WorkflowContextDelegation.java:108,119,127,…`). Resolved per-instance from `ProcessingContext.component(Clock.class)` (`WorkflowContextDelegation.java:108`). | **YES.** `Clock` is a first-class registered component; override with `componentRegistry(cr -> cr.registerComponent(Clock.class, c -> fixedClock))`. All time math already goes through it. Cleanest seam. |
| **RNG / UUID** | `UUID.randomUUID()` hardcoded at `ProcessingContextUtils.java:79` and `SimpleWorkflowExecution.java:509`. No RNG elsewhere. | **NO injection point.** Must be created (e.g. an injectable `Supplier<String>`/id source component) if deterministic UoW ids are required. workflowId derivation is already deterministic (`WorkflowIdProvider`), so this is low priority. |
| **Storage (event store + token + safe-point + exec repo)** | EventStore via `StreamableEventSource` component (`AllEventEventHandlingComponent.java:69`). Processor token store **hardcoded** `new InMemoryTokenStore()` (`AllEventEventHandlingComponent.java:70`). SafePoint store via `COMPONENT_SAFE_POINT_STORE` (`WorkflowConfigurationDefaults.java:161-179`). Exec repo via `WorkflowExecutionRepository` component (`:188-192`). | **MIXED.** EventStore, SafePointStore, ExecutionRepository, MutableWorkflowHistoryRepository are all replaceable components (interfaces + `registerComponent`/`registerIfNotPresent`). The **processor TokenStore is NOT injectable** at `AllEventEventHandlingComponent.java:70` (hardcoded `new InMemoryTokenStore()`) — a seam must be created there. `WorkflowReplayPreparedStateTest.java:241-252` proves EventStore/TokenStore/SafePointStore can be swapped via the configurer for DST-style tests. |
| **Queue / event-source** | Per-instance `ArrayBlockingQueue(1000)` constructed inline (`SimpleWorkflowExecution.java:92`). Event delivery via Axon PSEP (`WorkflowEventProcessingRegistrationEnhancer.java:102-157`). | **NO direct injection** of the task queue (private field, fixed capacity). Event source is a component (`StreamableEventSource`) and swappable (test uses `InMemoryEventStorageEngine`). For DST, driving events through `EventSink.publish(null, msg)` (as in `WorkflowReplayPreparedStateTest.java:286-292`) is the existing control surface. |
| **Scheduler / executor** | Engine body executor: `Executors.newVirtualThreadPerTaskExecutor()` (`WorkflowConfigurationDefaults.java:142-146`, name `WORKFLOW_ENGINE_EXECUTOR`). Timer scheduling: `CompletableFuture.delayedExecutor(...)` (ForkJoinPool) at `WaitForDelegate.java:138`, `RetryableExecuteDelegate.java:206`. | **PARTIAL.** The body `ExecutorService` is a registered component → replaceable with a deterministic/same-thread executor (`registerIfNotPresent(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR, …)`). The **`delayedExecutor` timer path is hardcoded** to the JDK common Delayer — a seam (e.g. an injectable scheduled executor / virtual time source) must be created to make timeouts/backoff deterministic. This is the highest-effort seam. |

---

## 13. Open questions / UNCLEAR

1. **UNCLEAR — durable leasing across nodes.** The processor's token store is hardcoded
   `new InMemoryTokenStore()` (`AllEventEventHandlingComponent.java:70`) and Spring autoconfig never
   overrides `TokenStore[Workflow]` (only the safe-point token store is overridden in bike-rental).
   I'd need to confirm whether (a) production is single-node by design, or (b) a durable
   `TokenStore[Workflow]` is expected to be injected elsewhere. This determines whether the TLA+ model
   may assume single-writer ownership or must model split-brain / two concurrent owners. (See FIXME
   `#190` at `AllEventEventHandlingComponent.java:72` and `WorkflowEventProcessingRegistrationEnhancer.java:210`.)
2. **RISK — side-effect at-most-once is NOT guaranteed.** `ExecuteDelegate.java:130` (`// FIXME ->
   consider to use QOS`) and the repeated `// FIXME - publish using an append condition`
   (`:163,173,178,183,188`). If a crash lands between `action.apply` (`:148`) and the `COMPLETED`
   commit, the step replays as `STARTED` and the action **re-executes**. The TLA+ invariant set should
   distinguish at-most-once *recording* (holds) from at-most-once *execution* (does NOT hold). Confirm
   intended QoS with the team.
3. **UNCLEAR — live-switch boundary race.** `checkAndCreateNewWorkflow` reads `isRunning.get()`
   (`WorkflowEngine.java:250`) to decide whether to `execute(...)` a freshly created instance, while
   `switchToLiveMode` flips `isRunning` and iterates `findAll()` (`:153,172`). Whether an instance
   created exactly at the boundary can be double-executed or skipped needs confirmation (no lock
   spans both paths). Candidate liveness/safety property for the model.
4. **UNCLEAR — task-queue overflow semantics.** `appendTask` throws `RuntimeException("Too many tasks
   …")` at capacity 1000 (`SimpleWorkflowExecution.java:472-477`, FIXME backpressure). Behavior of a
   workflow whose queue overflows (and whether that can happen during a burst of awaited events) is
   undefined — relevant if the model allows unbounded pending events.
5. **UNCLEAR — multiple wait conditions matching one event.** `EventWaitConditions.evaluateAndApply`
   iterates `entrySet()` in hash order and can complete several steps from one event
   (`EventWaitConditions.java:111-126`, `// TODO synchronized?`). Whether more than one wait condition
   per instance can legitimately match the same event (and the intended ordering) needs confirmation.
6. **UNCLEAR — exact Axon PSEP claim timeout / heartbeat parameters.** Lease acquire/expiry is owned
   by Axon's pooled-streaming processor, not this repo; the concrete claim-timeout values would come
   from Axon defaults/config outside this tree. Needed if the TLA+ model parameterizes lease expiry.
