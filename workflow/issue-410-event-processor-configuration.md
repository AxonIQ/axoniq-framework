# Configuring the workflow event processor (issue #410)

**Status:** proposal / handoff note. Not yet implemented — no source files have been changed as part of writing this
document.

**Audience:** contributors to the `workflow/*` modules. This doc exists so the design can be picked up by anyone on
the team without the original author present; it assumes familiarity with Axon Framework 5's configuration API
(`ComponentRegistry`, `ConfigurationEnhancer`, `EventProcessorModule`) but not with this specific investigation.

## TL;DR

- Issue: [#410](https://github.com/AxonIQ/axoniq-framework/issues/410) — `WorkflowEventProcessingRegistrationEnhancer`
  takes its settings as constructor arguments, so they can't vary per module and can't be read/overridden through the
  `Configuration` the way other Axon extensions can.
- The natural template is how `axoniq-dead-letter` extends `PooledStreamingEventProcessorConfiguration`
  (`DeadLetterQueueConfiguration` + `DeadLetterQueueConfigurationEnhancer`). That template applies cleanly to some of
  the enhancer's fields and not at all to one of them:
  - `initialSegmentCount` → **delete it**. It duplicates a setting `PooledStreamingEventProcessorConfiguration`
    already has, which Spring's `EventProcessorProperties` already resolves per processor name — the "can't differ
    per engine" complaint the issue raises is already solved by that generic mechanism. See Finding 1.
  - `engineComponentName` / `projectorComponentName` / `registerHistoryProjector` → **do** introduce a
    `ConfigurationExtension` for these, but only route their *lookup*-time usages through it. Their
    *structural* usages (declarative component names, whether the projector's component exists at all) can't move
    because of a timing constraint in the framework's builder API. See Finding 2.
- Net effect: one new small data class, one enhancer simplified (segment-count code deleted, lookup code reads from
  the new extension), two Spring-side files trimmed. No public API beyond `@Internal` classes changes shape in a
  way applications depend on.

## Background

Axon Framework 5's `ConfigurationExtension<P>` mechanism lets a module attach a typed, pure-data settings object onto
a parent configuration object (here, `PooledStreamingEventProcessorConfiguration`), registered via
`ExtensibleConfigurer.extend(Class, Supplier)` and read back via `ExtendedConfiguration.extension(Class)`. Both
methods are already available on every `PooledStreamingEventProcessorConfiguration` (they're declared on its base
class, `EventProcessorConfiguration`, in `axon-messaging`) — adopting the pattern needs no framework changes.

The reference implementation in this codebase is the dead-letter-queue module:

- `messaging/axoniq-dead-letter/src/main/java/io/axoniq/framework/messaging/eventhandling/deadletter/DeadLetterQueueConfiguration.java`
  implements `ConfigurationExtension<PooledStreamingEventProcessorConfiguration>`. It's pure data — fluent setters,
  `name()`, `validate()`, `describeTo()` — with no behavior.
- `.../DeadLetterQueueConfigurationEnhancer.java` implements `ConfigurationEnhancer`. It's a separate, always-active
  enhancer that installs decorators/factories which check `processorConfig.extension(DeadLetterQueueConfiguration.class)`
  at runtime and act only when the extension says so (e.g. `dlqConfig.isEnabled()`).
- Registration example, from `DeadLetterQueueConfiguration`'s own Javadoc:
  ```java
  config.extend(DeadLetterQueueConfiguration.class, () -> new DeadLetterQueueConfiguration()
            .enabled()
            .enqueuePolicy((letter, cause) -> Decisions.enqueue(cause))
            .clearOnReset(false)
            .cacheMaxSize(2048)
  );
  ```

This is the shape issue #410 was implicitly asking the workflow module to adopt.

## Current state of `WorkflowEventProcessingRegistrationEnhancer`

File:
`workflow/axoniq-workflow-engine/src/main/java/io/axoniq/framework/workflow/configuration/WorkflowEventProcessingRegistrationEnhancer.java`

It takes five constructor arguments: `moduleName`, `engineComponentName`, `projectorComponentName`,
`registerHistoryProjector`, and (via a second, overloaded constructor) `initialSegmentCount`. Three call sites
construct it:

| Call site | File | Segment count | Component names |
|---|---|---|---|
| `WorkflowConfigurer.enhance()` | `configuration/WorkflowConfigurer.java` | never set (4-arg ctor) | always `null`/`null` — the default `"Workflow"` module |
| `SimpleWorkflowModule.registerGivenComponents()` | `configuration/SimpleWorkflowModule.java` | never set (4-arg ctor) | its own `COMPONENT_WORKFLOW_ENGINE` / `COMPONENT_WORKFLOW_HISTORY_PROJECTOR` per named module |
| `WorkflowAutoConfiguration.workflowEventHandlersDefaults(...)` | `workflow/axoniq-workflow-spring-boot/.../WorkflowAutoConfiguration.java` | from `WorkflowProperties.getInitialSegmentCount()` (5-arg ctor) | always `null`/`null` |

Two methods inside the enhancer consume these fields, and they run at **different times relative to
`Configuration`'s existence** — this distinction drives everything below:

1. **`eventHandlingComponents()`** builds the `Function<RequiredComponentPhase, CompletePhase>` passed to
   `EventProcessorModule.pooledStreaming(moduleName).eventHandlingComponents(...)`. This executes synchronously
   inside `enhance(ComponentRegistry)`, **before any `Configuration` exists**. It uses:
   - `engineComponentName` / `projectorComponentName` to build the **declarative component name strings**, e.g.
     `engineComponentName != null ? engineComponentName + "ExecutionEventing" : DEFAULT_MODULE_NAME + "ExecutionEventing"`.
   - `registerHistoryProjector` to decide **whether `.declarative(...)` is called for the projector at all**.

   Both of these are plain Java `if`/string-concatenation decisions over already-known values — nothing in
   `RequiredComponentPhase.declarative(String componentName, ComponentBuilder<EventHandlingComponent> builder)`
   (`org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer`) gives access to a
   `Configuration` at this point; `componentName` is a bare `String`.

2. **`processorCustomization()`** — the `BiFunction<Configuration, PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration>`
   passed to `.customized(...)` — and the `cfg -> {...}` lambdas inside `eventHandlingComponents()`'s declarative
   builders, all run **with a `Configuration` already available**. These use the same fields differently:
   - `workflowEngine(Configuration cfg)` looks up a named or unnamed `WorkflowEngine` component.
   - A branch inside the engine's declarative lambda picks between two `EventHandlingComponentHandlingAny`
     constructors depending on whether `engineComponentName != null`.
   - The projector's declarative lambda looks up a named or unnamed `WorkflowHistoryProjector`.
   - `processorCustomization()` previously also called `withSegmentCount(processorConfiguration)`, which just
     applied `initialSegmentCount` if non-null.

## Finding 1 — `initialSegmentCount` is already solved generically; delete it, don't extend it

`PooledStreamingEventProcessorConfiguration`
(`org/axonframework/messaging/eventhandling/processing/streaming/pooled/PooledStreamingEventProcessorConfiguration.java`
in `axon-messaging`) already declares `initialSegmentCount` as a first-class field (default `16`), with its own
public `initialSegmentCount(int)` setter and `initialSegmentCount()` getter. Axon's Spring integration
(`EventProcessorProperties`, `EventProcessorSettings`, `EventProcessingAutoConfiguration`, all in
`axon-spring-boot-autoconfigure`) already resolves `axon.eventhandling.processors.<name>.initial-segment-count` **per
named processor**, and applies it before/around any `.customized(...)` callback runs — including a workflow module's
processor, since the framework has no notion of "workflow processor" being special; it's just another named
`PooledStreamingEventProcessorConfiguration`.

Concretely, this means:

- By the time `processorCustomization()`'s `processorConfiguration` argument reaches the workflow enhancer, it
  **already carries** whatever segment count Spring resolved for that module's processor name, or whatever a
  programmatic override set via
  `configurer.eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(...).customized((cfg, c) -> c.initialSegmentCount(8))))`.
  The enhancer's own comment on `withSegmentCount` already half-admits this: *"the processor keeps the count it
  already carries, which is the default of the event processing configuration or whatever a customization of this
  application set."*
- This is **already keyed by processor/module name** in `EventProcessorProperties`, so it already differs per
  engine, with zero workflow-specific code — two differently-named workflow modules already get independent
  segment counts today via two differently-named Spring properties.
- The workflow-specific `initialSegmentCount` constructor argument, the `withSegmentCount` method, and
  `WorkflowProperties.initialSegmentCount` / `axoniq.workflow.initial-segment-count` are **pure duplication** of a
  path that already works and already solves the issue's "cannot differ per engine" complaint.

**Conclusion: do not wrap `initialSegmentCount` in a `ConfigurationExtension`. Remove it.** Introducing one would add
a second, redundant path for a setting that already has a generic, already-wired first-class home.

### What to change

- `WorkflowEventProcessingRegistrationEnhancer`: delete the `initialSegmentCount` field, the 5-arg constructor that
  sets it, and the `withSegmentCount` method. `processorCustomization()` stops touching segment count entirely.
- `WorkflowProperties` (`workflow/axoniq-workflow-spring-boot/.../WorkflowProperties.java`): delete the
  `initialSegmentCount` field — it's the only field the class has today, so the team should decide whether to delete
  `WorkflowProperties` outright or keep it empty as a placeholder for future workflow-only Spring properties.
- `WorkflowAutoConfiguration`: `workflowEventHandlersDefaults(...)` drops the `WorkflowProperties` parameter and uses
  the 4-arg constructor. Drop `@EnableConfigurationProperties(WorkflowProperties.class)` too if the class is deleted.
- Document that a workflow module's segment count (and batch size, thread count, token claim interval, etc.) is
  configured exactly like any other event processor's: `axon.eventhandling.processors.<moduleName>.*`, where
  `<moduleName>` defaults to `"Workflow"` (`WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME`) or
  whatever name a `WorkflowModule` was registered under.
- Update any `docs/` page that currently mentions `axoniq.workflow.initial-segment-count`.

## Finding 2 — the other three fields: extend the lookup half, keep the structural half as-is

`engineComponentName`, `projectorComponentName`, and `registerHistoryProjector` have no generic home the way segment
count does — they're genuinely workflow-specific wiring, so the `ConfigurationExtension` recipe is the right fit.
But — per the timing distinction above — only the *lookup*-time usages can move there. The *structural* usages
(declarative name strings, whether the projector's declarative component is registered at all) run before any
`Configuration` exists and can't be deferred without a much larger restructure (see "Rejected alternative" below).

### New class: `WorkflowEventProcessorConfiguration`

New file, same package as the enhancer, modeled directly on `DeadLetterQueueConfiguration`'s shape:

```java
public class WorkflowEventProcessorConfiguration
        implements ConfigurationExtension<PooledStreamingEventProcessorConfiguration> {

    @Nullable
    private String engineComponentName;
    @Nullable
    private String projectorComponentName;
    private boolean registerHistoryProjector;

    public WorkflowEventProcessorConfiguration engineComponentName(@Nullable String engineComponentName) {
        this.engineComponentName = engineComponentName;
        return this;
    }

    @Nullable
    public String engineComponentName() {
        return engineComponentName;
    }

    public WorkflowEventProcessorConfiguration projectorComponentName(@Nullable String projectorComponentName) {
        this.projectorComponentName = projectorComponentName;
        return this;
    }

    @Nullable
    public String projectorComponentName() {
        return projectorComponentName;
    }

    public WorkflowEventProcessorConfiguration registerHistoryProjector(boolean registerHistoryProjector) {
        this.registerHistoryProjector = registerHistoryProjector;
        return this;
    }

    public boolean registerHistoryProjector() {
        return registerHistoryProjector;
    }

    @Override
    public String name() {
        return "workflowEventProcessor";
    }

    @Override
    public void validate() {
        // No invariants yet — all combinations of these three fields are valid.
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("engineComponentName", engineComponentName);
        descriptor.describeProperty("projectorComponentName", projectorComponentName);
        descriptor.describeProperty("registerHistoryProjector", registerHistoryProjector);
    }
}
```

### Enhancer changes

The enhancer **keeps** its `engineComponentName` / `projectorComponentName` / `registerHistoryProjector` constructor
parameters and private fields unchanged — they're still required for the structural decisions in
`eventHandlingComponents()`. What changes is that `processorCustomization()` now also seeds the extension from those
same fields:

```java
private BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
        PooledStreamingEventProcessorConfiguration> processorCustomization() {
    return (cfg, processorConfiguration) -> {
        processorConfiguration.extend(
                WorkflowEventProcessorConfiguration.class,
                () -> new WorkflowEventProcessorConfiguration()
                        .engineComponentName(engineComponentName)
                        .projectorComponentName(projectorComponentName)
                        .registerHistoryProjector(registerHistoryProjector)
        );
        return processorConfiguration
                .eventCriteria(set -> set.isEmpty()
                        ? EventCriteria.havingAnyTag()
                        : EventCriteria.havingAnyTag().andBeingOneOfTypes(set))
                .eventSource(cfg.getComponent(StreamableEventSource.class))
                .tokenStore(cfg.getOptionalComponent(TokenStore.class).orElseGet(() -> {
                    logger.warn(/* unchanged */);
                    return new InMemoryTokenStore();
                }))
                .unitOfWorkFactory(cfg.getComponent(UnitOfWorkFactory.class))
                .addSegmentChangeListener(segmentChangeListener(cfg));
    };
}
```

Then `workflowEngine(Configuration cfg)` and the two lookup call sites inside `eventHandlingComponents()`'s lambdas
read the extension instead of closing over `this.engineComponentName` / `this.projectorComponentName`:

```java
private WorkflowEngine workflowEngine(Configuration cfg) {
    String engineComponentName = cfg.getComponent(PooledStreamingEventProcessorConfiguration.class)
                                     .extension(WorkflowEventProcessorConfiguration.class)
                                     .engineComponentName();
    return engineComponentName != null
            ? cfg.getComponent(WorkflowEngine.class, engineComponentName)
            : cfg.getComponent(WorkflowEngine.class);
}
```

(`PooledStreamingEventProcessorConfiguration` is resolvable this way from any `Configuration` within the module's own
scope — the same lookup `DeadLetterQueueConfigurationEnhancer` already relies on in
`decorateWithDeadLettering`/`DeadLetterQueueComponentFactory.construct`.)

`eventHandlingComponents()` itself — the declarative name strings and the `if (registerHistoryProjector)` branch that
decides whether the projector's component is registered at all — **stays exactly as it is today**, reading the
private fields directly, because those decisions run before `Configuration` exists.

### Be explicit with the team about the tradeoff

This does **not** let a workflow module's engine/projector component names or history-projector toggle be supplied
any other way than construction — the enhancer's constructor keeps all three parameters, because the framework's
builder API resolves module structure before a `Configuration` exists. What it *does* buy:

- Every lookup-time consumer stops reading a captured Java field and reads the same kind of
  `Configuration`-resident object that DLQ-style code already knows how to read (`.extension(...)`) — consistent
  with the rest of the framework's extension convention.
- Anything that already holds a `PooledStreamingEventProcessorConfiguration` (tests, future enhancers, diagnostics)
  can inspect what a given workflow processor resolved to via `.extension(WorkflowEventProcessorConfiguration.class)`,
  without reaching into enhancer internals.
- Forward compatibility: if `EventHandlingComponentsConfigurer` is ever extended to support Configuration-aware
  naming, the constructor parameters could be dropped entirely at that point, with this extension already the single
  source of truth.

### Rejected alternative: full DLQ parity

DLQ's own decorator is **always installed** and behaves as a pass-through when its extension says disabled — the
"fully general" version of this pattern would be to always register the projector's declarative component too, and
gate its actual behavior (and possibly derive both engine/projector component names purely from `moduleName`,
dropping the separate override fields) through the extension read inside the lambda, matching DLQ's shape exactly.

This was considered and set aside: it would add exactly the kind of unconditional module structure that
[#427](https://github.com/AxonIQ/axoniq-framework/issues/427)'s investigation already found this module to be
fragile around — that issue's disabled `WorkflowConfigurerTest` fails against the generic
`ApplicationConfigurerTestSuite` specifically because of exact-count assumptions about registered
modules/enhancers/components once the workflow module's structure isn't minimal. Revisit this option only after
#427 is resolved, if there's a concrete need for it.

## Relation to #427

[GitHub #427](https://github.com/AxonIQ/axoniq-framework/issues/427) tracks the disabled `WorkflowConfigurerTest`
(`workflow/axoniq-workflow-engine/src/test/...`), which inherits `ApplicationConfigurerTestSuite` but fails because
`WorkflowConfigurer.create()` isn't a blank configurer — it wraps `EventSourcingConfigurer` and auto-registers the
default workflow event-processing module, breaking the suite's exact-count assumptions. That issue's own comment
thread concludes the fix is targeted overrides in `WorkflowConfigurerTest`, not changes to `WorkflowConfigurer.create()`.
It's referenced twice in this doc (also in Finding 2) because it's the concrete reason the "full DLQ parity" option
was set aside. Issue #410's own comment thread suggested tackling #410 and #427 together for configuration
consolidation — this doc's changes are compatible with that either way, since they don't alter
`WorkflowConfigurer.create()`'s observable behavior.

## Files touched (when implemented)

- `workflow/axoniq-workflow-engine/src/main/java/io/axoniq/framework/workflow/configuration/WorkflowEventProcessorConfiguration.java` — **new**
- `workflow/axoniq-workflow-engine/src/main/java/io/axoniq/framework/workflow/configuration/WorkflowEventProcessingRegistrationEnhancer.java`
  — remove `initialSegmentCount` (field, 5-arg constructor, `withSegmentCount`); add extension-seeding in
  `processorCustomization()`; switch lookup call sites to read from the extension.
- `workflow/axoniq-workflow-spring-boot/src/main/java/io/axoniq/framework/workflow/springboot/WorkflowProperties.java`
  — remove `initialSegmentCount` (its only field today).
- `workflow/axoniq-workflow-spring-boot/src/main/java/io/axoniq/framework/workflow/springboot/WorkflowAutoConfiguration.java`
  — `workflowEventHandlersDefaults(...)` drops the `WorkflowProperties` parameter; drop
  `@EnableConfigurationProperties(WorkflowProperties.class)` if `WorkflowProperties` is deleted.
- Tests:
  - New `WorkflowEventProcessorConfigurationTest`, mirroring
    `messaging/axoniq-dead-letter/src/test/java/io/axoniq/framework/messaging/eventhandling/deadletter/DeadLetterQueueConfigurationTest.java`'s
    shape (fluent setters, `validate()`, `describeTo()`).
  - Update `WorkflowEventProcessingRegistrationEnhancerTest` to cover the extension being seeded and readable via
    `processorConfiguration.extension(WorkflowEventProcessorConfiguration.class)` after `processorCustomization()`
    runs, and remove any assertions tied to the deleted segment-count behavior.
  - Check `WorkflowAutoConfiguration` / `WorkflowProperties` Spring tests for references to the removed constructor,
    bean signature, or `axoniq.workflow.initial-segment-count` property.
- Any `docs/` page mentioning `axoniq.workflow.initial-segment-count` — point at
  `axon.eventhandling.processors.<name>.*` instead.

**No changes needed** in `SimpleWorkflowModule` or `WorkflowConfigurer` — both keep calling the same 4-arg
constructor; the extension-seeding happens entirely inside the enhancer.

## Verification (when implemented)

- `./mvnw test -pl workflow/axoniq-workflow-engine -Dtest=WorkflowEventProcessingRegistrationEnhancerTest,WorkflowEventProcessorConfigurationTest`
- `./mvnw test -pl workflow/axoniq-workflow-spring-boot`
- `./mvnw clean verify` across the workflow modules, to confirm nothing else references the removed constructor or
  the removed `WorkflowProperties` field.
- Manually confirm a workflow module's segment count is still controllable via
  `axon.eventhandling.processors.Workflow.initial-segment-count` (or a custom module name) in an example app.