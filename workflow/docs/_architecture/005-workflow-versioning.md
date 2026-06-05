# ADR 005: Workflow Versioning

- Status: Accepted
- Date: 2026-05-14 (revised 2026-05-27)

## Context

In-flight workflows replay from event history when their declaring code changes. The engine's
name-based step lookup tolerates additive changes (new steps, re-orderings, removals) but not
semantic changes — a step whose body now talks to a different service, or a branching change in the
workflow body. Without an explicit fork mechanism, such changes silently corrupt in-flight workflows.

Two complementary classes of code change need first-class support: additive in-place changes
(insert a step, fork a branch on payload state) and whole-body rewrites (copy the class, bump the
version, let new instances run on the new code while old instances finish on the old one). We need
a model that fits Axon's event-sourced, name-based replay machinery and never silently corrupts
in-flight workflows.

## Decision

Workflow versioning is built from three layered mechanisms that share a single semver-string
namespace.

### 1. Definition-level version

`@Workflow(workflowVersion = "1.0.0")` (and the programmatic
`WorkflowCustomization.workflowVersion(...)` equivalent) declares a semver string validated by
`Version` at registration time. Default is `MessageType.DEFAULT_VERSION`
(`"0.0.1"`), so existing definitions need no source change.

### 2. Event versioning via AF5 `MessageType.version()`

Every event a workflow emits is constructed as `new MessageType(name, state.workflowVersion())`.
The workflow's version travels on the event envelope's `MessageType.version()` — AF5's standard
event-versioning channel — not as a separate metadata key. `state.workflowVersion()` is seeded
from the spawn-time definition, then overwritten during `evolve(...)` from the started event's own
`MessageType.version()` so replays adopt the version the workflow was originally launched under.

### 3. `ctx.migrateVersion(changeId, newVersion)` primitive

```java
if (ctx.migrateVersion("payment-redesign", "1.0.1")) {
    ctx.awaitExecute("processPayment", PaymentService::processV2);
} else {
    ctx.awaitExecute("chargePayment", PaymentService::chargeV1);
}
```

Returns `true` iff the workflow has committed to (or is past) `newVersion` for this `changeId`,
`false` if it stays on the legacy branch. Concretely:
- Already recorded for `changeId` → `true` iff the recorded version is `>=` `newVersion`.
- Not recorded but workflow already at `newVersion` → `true` (no redundant publish).
- Not recorded, `newVersion` strictly greater than current → publishes a migration step and
  returns `true`; if the replay-drift guard fires (in-flight workflow already ran past this
  point), returns `false`.
- Downgrades (requested `<` current, no recorded step) raise `IllegalArgumentException`.

Call at most once per `changeId` per workflow body.

#### Downstream-steps guard (correctness invariant)

`ctx.migrateVersion(...)` returns `false` (and emits nothing) if state contains any terminal step
the current invocation has not yet referenced. Those untouched-but-present steps prove old code
already executed past this point, so the workflow stays on the legacy branch. Powered by a
per-invocation `Set<String> referencedStepNames` populated by every event-emitting primitive.

### 4. Multi-version routing

When multiple definitions share the same `(workflowName, startOnEventName, idProperty)` triple but
differ by version, the engine: spawns new instances on the highest registered version, augments
workflowIds with `#<version>` to keep two versions running in parallel for the same domain key,
and dispatches the body of an existing instance through a four-pass lookup
(exact-match-spawn-config → exact-match-sibling → closest-sibling ≤ recorded state →
closest-higher-sibling > recorded state). The last tier covers the "annotation bumped past the
version on in-flight workflows" case — single registered definition at a newer version, state still
at the old version. Full passes and worked examples live in the
[versioning reference doc](../reference/modules/workflows/pages/versioning.adoc).

### 5. Drift-detection safety net (universal)

For developers who do not opt in to versioning, the downstream-steps guard fires from every
event-emitting primitive before publishing the first event for a step not already in state. When
the guard fires it throws `WorkflowReplayDriftException`, handled non-terminally: a warning is
logged and no terminal workflow event is published. The workflow stays at its current state.
Recovery is by reverting the offending code OR wrapping the change in `ctx.migrateVersion(...)`; the next
replay then runs cleanly. `ctx.migrateVersion(...)` itself runs the same check but returns the current
version unchanged rather than throwing — that's the entire purpose of the primitive.

## Consequences

- Workflow authors get one consistent semver-string namespace covering the `@Workflow` attribute,
  the programmatic setter, the `ctx.migrateVersion(...)` primitive, every emitted event's
  `MessageType.version()`, and the registry's lookup helpers.
- New public API surface: `Workflow.workflowVersion()`, `WorkflowContext.workflowVersion()`,
  `WorkflowState.workflowDefinitionVersion()` / `currentWorkflowVersion(changeId)` /
  `hasVersionMigrationStep(changeId)`,
  `WorkflowConfiguration.workflowVersion()` and `WorkflowCustomization.workflowVersion(String)`,
  `WorkflowConfigurationRegistry.getHighestVersionConfigurations(...)`,
  `findByWorkflowNameAndVersion(...)`, `findClosestRegisteredVersion(...)`,
  `Version.of(String)` / `Version.validate(String)`.
- `WorkflowExecution` gains `recordStepReference` / `referencedStepNames` powering the guard;
  every event-emitting delegate records a reference on each step encounter.
- Spring Boot autoconfig groups `@Workflow` beans of the same context type into a SINGLE
  `WorkflowModule` so all version variants share one engine + registry + repository — the
  architectural prerequisite for multi-version routing.
- Mandatory rule: call `ctx.migrateVersion(changeId, newVersion)` at most once per `changeId` per
  workflow body; multiple calls could see different `referencedStepNames` snapshots.
- First-writer wins per `changeId`: once a migration step is recorded, the boolean return is
  derived from the recorded value (`recorded >= requested`), so changing the version number in
  later deploys does not publish a second step for the same `changeId`.
- Step renames, payload-shape changes, and side-effecting step removals remain forbidden
  migrations — the prescribed remedy is a new workflow type (new workflow name).

## Trade-offs

- Every emitted event carries the workflow version on its `MessageType` — AF5's standard
  event-versioning channel; same cost as any non-default AF5 versioning.
- Downgrade attempts on `ctx.migrateVersion(...)` raise `IllegalArgumentException` rather than silently
  no-op; noisy failure is preferred because a downgrade almost always signals a bug.
- Same-version duplicates run in parallel (with a startup warning) rather than dedupe, preserving
  the deliberate "blue/green inside one binary" pattern.
- Cross-version disambiguation uses `#` as the workflowId separator; callers surfacing workflow
  ids over HTTP query strings should URL-encode them.
