# Plan: Issue #412 — Make workflow-annotated methods decoratable through the framework

## Context

Workflow-annotated methods (`@Workflow` bodies and the `@WorkflowStatusChangedHandler` family of
lifecycle handlers) are invoked today by raw `Method.invoke` reflection
(`WorkflowReflectionUtils.invoke`), completely bypassing the Axon Framework handler pipeline. Even
though the annotations *look* like Axon Framework handler annotations, there is no
`MessageHandlingMember`, no `HandlerDefinition`, and no seam for `HandlerEnhancerDefinition` to
wrap them — so tracing, `@MessageHandlerInterceptor`, handler timeouts, and replay-awareness are
all unavailable to workflow authors. Raised by Steven van Beelen in the Framework Team weekly of
2026-08-17 to restore API consistency with the rest of Axon Framework.

**This is the third iteration of this plan.** Each iteration simplified further:

1. First draft: two separate seams — a `HandlerDefinition`/`HandlerEnhancerDefinition` route for
   lifecycle handlers, plus a brand-new bespoke `WorkflowDefinitionEnhancerDefinition` SPI for the
   body.
2. Second draft: one shared internal helper hand-builds a `MethodInvokingMessageHandlingMember` and
   manually applies `configuration.getComponent(HandlerEnhancerDefinition.class)` for both call
   sites — no new public SPI, but still our own bespoke annotation scanning
   (`AutoDetectionUtils.workflowMethods`/`annotatedMethods`/`hasContextParameter`).
3. **This draft: reuse `AnnotatedHandlerInspector` for the scanning and construction itself** — the
   exact same declarative machinery Axon Framework uses for Command Handling Components, Event
   Sourced Entities, and every other annotation-driven handler. This eliminates our own reflection
   scanning almost entirely and gets `@MessageHandlerInterceptor` chain-wiring for free, matching
   how the rest of the framework works instead of approximating it.

Nothing under `workflow/` has been publicly released yet (still `5.4.0-SNAPSHOT`), so the module's
public API is free to change — no compatibility constraint forces an additive-only change.

All findings below were verified by reading the actual Axon Framework source (sibling checkout,
`org.axonframework:axon-messaging`/`axon-eventsourcing`, external jar dependencies of this module) —
not just the GitHub issue text, which has several stale line numbers/package names.

---

## The core mechanism, verified against Axon Framework's own source

Traced exactly how `AnnotatedCommandHandlingComponent` (public, `@since 0.5.0`) is built, since it's
the canonical example of "declarative annotation scanning → enhanced, interceptor-aware handlers"
that the user asked to mirror:

1. **`AnnotatedHandlerInspector.inspectType(Class<T> handlerType, MessageTypeResolver, ParameterResolverFactory, HandlerDefinition)`**
   (`org.axonframework.messaging.core.annotation`, `@Internal`, `@since 3.0.0`) takes a
   **caller-supplied `HandlerDefinition`** — it does not require methods to be `@MessageHandler`-annotated
   in any framework-wide sense; it just asks the given `HandlerDefinition.createHandler(...)`
   whether it recognizes each declared method (across the full type/interface/superclass hierarchy —
   `initializeMessageHandlers` walks `getDeclaredMethods()` plus super/subclass inspectors
   recursively). This means **we can write our own tiny `HandlerDefinition` that recognizes
   `@Workflow` and `@WorkflowStatusChangedHandler` directly, with zero need to meta-annotate them as
   `@MessageHandler`**, and zero risk of them being picked up by any unrelated Axon Framework
   component scanning (our `HandlerDefinition` is never registered globally, only ever passed
   directly into our own local `inspectType(...)` call).
2. **Enhancer-wrapping happens in `MultiHandlerDefinition`, not in the inspector or in
   `AnnotatedCommandHandlingComponent`.** Verified in `MultiHandlerDefinition.createHandler`: it
   tries each delegate `HandlerDefinition` in order, and the moment one returns a match, wraps it
   exactly once via `handlerEnhancerDefinition.wrapHandler(...)` before returning. This is precisely
   the idiom `HandlerDefinitionUtils.registerToComponentRegistry` already uses framework-wide:
   `MultiHandlerDefinition.ordered(configuration.getComponent(HandlerEnhancerDefinition.class), ...delegates)`.
   We reuse this idiom locally (not registered in `ComponentRegistry` — built once, inline, for our
   own `inspectType(...)` call).
3. **`@MessageHandlerInterceptor` support comes free** if the standard
   `AnnotatedMessageHandlingMemberDefinition` (the framework's default `@MessageHandler`-recognizing
   `HandlerDefinition`) is included as a second delegate. `AnnotatedHandlerInspector.registerHandler`
   automatically routes any member unwrapping to `MessageInterceptingMember` into a separate
   `interceptors` bucket, and `inspector.chainedInterceptor(type)` returns a real, populated
   `MessageHandlerInterceptorMemberChain` when interceptor-annotated methods exist on the class —
   otherwise `NoMoreInterceptors.instance()` (a safe, cost-free no-op). Including this delegate is
   safe even though it also makes the inspector "see" any incidental `@CommandHandler`/`@EventHandler`/
   `@QueryHandler` a user might separately declare on the same class, because we always additionally
   filter `getUniqueHandlers(...)` down to members whose underlying method actually carries
   `@Workflow`/`@WorkflowStatusChangedHandler` before using them — anything else is simply ignored,
   not misfired.
4. **Invocation goes through the interceptor chain, not the member directly** — verified in
   `AnnotatedCommandHandlingComponent.constructCommandHandlerFor`:
   `model.chainedInterceptor(target.getClass()).handle(message, context, target, handler)`. This
   `MessageHandlerInterceptorMemberChain.handle(Message, ProcessingContext, T target, MessageHandlingMember<? super T> handler)`
   call threads the message through any `@MessageHandlerInterceptor` methods first, then the real
   handler — this is the call shape both Part A and Part B adopt below, replacing the earlier
   drafts' direct `member.handle(...)` call.
5. `ParameterResolverFactory` remains completely orthogonal to all of the above — it resolves each
   parameter of whichever method got recognized, regardless of which `HandlerDefinition` recognized
   it. The workflow module's own `WorkflowMethodParameterResolverFactory` (below) is unchanged in
   role.

**What this eliminates from the previous draft:** `AutoDetectionUtils.workflowMethods`,
`annotatedMethods`, `hasContextParameter`, the raw `ReflectionUtils.methodsOf(type)`/`getMethods()`
manual stream-scanning, and the bespoke `WorkflowHandlingMemberFactory.buildEnhancedMember` helper
that manually called `handlerEnhancerDefinition.wrapHandler(...)` — all replaced by one
`AnnotatedHandlerInspector` per workflow-annotated class, built once at configuration time,
mirroring `AnnotatedCommandHandlingComponent`'s own construction almost line for line.

**What stays exactly as in the previous draft, and why:**
- **Message vehicle split.** Lifecycle handlers use the *real* `EventMessage` that drove the status
  transition (genuinely a reaction to something that happened, genuinely replayable — a tracking
  processor reset really does replay it). The body uses a *synthesized* `CommandMessage<WorkflowContext>`
  (a request to resume/run the workflow now, never stored, never bus-dispatched). This becomes the
  `messageType` our custom `HandlerDefinition` assigns per annotation, not a call-site choice — see
  below.
- **`WorkflowContext` stays untouched, not a `Message`.** It's a stateful capability/facade object,
  architecturally closer to `ProcessingContext` than to `Message`; only ever used as the *payload* of
  the internally-synthesized `CommandMessage` for the body.
- **Public workflow vocabulary stays Axon-agnostic.** `WorkflowContext`, `WorkflowDefinition`,
  `WorkflowStatusChangeListener`, and the lifecycle annotations remain free of Axon
  `Message`/`MessageHandlingMember`/`ProcessingContext` types in their own signatures. Everything
  described in this plan — the custom `HandlerDefinition`, the `AnnotatedHandlerInspector` usage, the
  synthesized messages — lives in `@Internal` adapter classes under `configuration`/`runtime.util`.
  Extension authors who want to affect workflow-annotated methods still register a standard Axon
  `HandlerEnhancerDefinition`, the same mechanism used for every other Axon handler type — an
  intentional, unavoidable extension point for that audience, not a leak into the domain API.

---

## Part A — The custom `HandlerDefinition` and shared `AnnotatedHandlerInspector`

New `@Internal` class, `WorkflowAnnotatedHandlerDefinition implements HandlerDefinition`, in
`io.axoniq.framework.workflow.configuration`:

```java
@Override
public <T> Optional<MessageHandlingMember<T>> createHandler(
        Class<T> declaringType, Method method,
        ParameterResolverFactory parameterResolverFactory,
        Function<Object, MessageStream<?>> messageStreamResolver) {
    if (AnnotationUtils.findAnnotationAttributes(method, Workflow.class).isPresent()) {
        return Optional.of(new MethodInvokingMessageHandlingMember<>(
                method, CommandMessage.class, Object.class, parameterResolverFactory, messageStreamResolver));
    }
    if (AnnotationUtils.findAnnotationAttributes(method, WorkflowStatusChangedHandler.class).isPresent()) {
        return Optional.of(new MethodInvokingMessageHandlingMember<>(
                method, EventMessage.class, Object.class, parameterResolverFactory, messageStreamResolver));
    }
    return Optional.empty();
}
```

This directly replaces `AutoDetectionUtils.workflowMethods`'s `hasContextParameter`-based filter and
`detectAndAddListener`'s annotation walk — the same `AnnotationUtils.findAnnotationAttributes` calls
already used today, just relocated into a `HandlerDefinition`, which is the idiomatic seam for
"recognize this annotation, build a member" in Axon Framework.

**Do not override `ParameterResolver.supportedPayloadType()`** on any workflow parameter resolver —
leave the default (`Object.class`). Verified: `MethodInvokingMessageHandlingMember`'s constructor
cross-checks every parameter resolver's `supportedPayloadType()` against the others and throws
`UnsupportedHandlerException` on conflict; only the `Object.class` default keeps a method with both a
`WorkflowContext` and a `WorkflowStatus` parameter from tripping that check.

`AutoDetectingWorkflowBuilder`'s constructor (where `instance` is already built once per module,
line ~75) builds one inspector per instance class, reused for every workflow method on it:

```java
var combinedHandlerDefinition = MultiHandlerDefinition.ordered(
        configuration.getComponent(HandlerEnhancerDefinition.class),
        new WorkflowAnnotatedHandlerDefinition(),
        new AnnotatedMessageHandlingMemberDefinition()   // recognizes @MessageHandlerInterceptor on the same class
);
var inspector = AnnotatedHandlerInspector.inspectType(
        instance.getClass(),
        configuration.getComponent(MessageTypeResolver.class),
        configuration.getComponent(ParameterResolverFactory.class),
        combinedHandlerDefinition
);
```

---

## Part B — Lifecycle handlers

### B.1 Thread the real `EventMessage`/`ProcessingContext` through the notify chain

File: `workflow/axoniq-workflow-engine/src/main/java/io/axoniq/framework/workflow/runtime/execution/EventSourcedWorkflowState.java`

Unchanged from the previous draft:
- `evolve(EventMessage, ProcessingContext, boolean notifyStatusListeners)` (line ~269) already has
  both in scope; pass both through its `setStatus(...)` call (line 382).
- `setStatus(...)` (line 457) gains `EventMessage eventMessage, ProcessingContext processingContext`
  params; inside `if (notifyStatusListeners)` (line 464), call
  `this.listenerSupport.notify(workflowStatus, eventMessage, processingContext)`.
- `WorkflowStateListenerSupport.notify(WorkflowStatus)` (nested record, line 531) gains the same two
  params, calling the new 4-arg `onWorkflowStatus` overload (below).
- The rehydration call sites passing `notifyStatusListeners = false` (`SimpleWorkflowExecution.java:507,510`)
  are unaffected — the guard at line 464 still short-circuits before `notify` is reached.

### B.2 Additive, non-breaking `WorkflowStatusChangeListener` signature

File: `.../runtime/api/execution/context/WorkflowStatusChangeListener.java`

```java
@FunctionalInterface
public interface WorkflowStatusChangeListener {

    <C extends WorkflowContext> void onWorkflowStatus(WorkflowStatus state, C context);

    default <C extends WorkflowContext> void onWorkflowStatus(
            WorkflowStatus state, C context, EventMessage<?> eventMessage, ProcessingContext processingContext) {
        onWorkflowStatus(state, context);
    }
}
```

A default-method overload keeps `CompositeWorkflowStatusChangeListener` and any hand-written
programmatic listener working with a minimal diff, even though the module is free to make breaking
changes pre-release. Update `CompositeWorkflowStatusChangeListener` with a matching 4-arg override
fanning out to each delegate (keep the 2-arg override as-is).

### B.3 Build and index the lifecycle-handler members

File: `.../configuration/AutoDetectionUtils.java`

`statusChangeListeners(...)`/`detectAndAddListener(...)` are replaced by logic that:
1. Calls `inspector.getUniqueHandlers(instance.getClass(), EventMessage.class)`.
2. Filters to members whose `.unwrap(Executable.class)` carries `@WorkflowStatusChangedHandler`
   (directly or meta-annotated via the 5 shortcut annotations — same
   `AnnotationUtils.findAnnotationAttributes` walk used today).
3. For each match, reads `workflowStatus()`/`workflowName()` attributes (same as today's
   `AutoDetectionUtils.validateAttributes`/attribute extraction) and builds a
   `Map<WorkflowStatus, MessageHandlingMember<Object>>`.

New `@Internal` `MessageHandlingWorkflowStatusChangeListener implements WorkflowStatusChangeListener`
wraps the built map + `inspector` + `instance`:

```java
@Override
public <C extends WorkflowContext> void onWorkflowStatus(WorkflowStatus state, C context) {
    // legacy fallback: no message/context available — degrades to the same hand-rolled
    // reflection invocation as today via WorkflowReflectionUtils.invoke.
}

@Override
public <C extends WorkflowContext> void onWorkflowStatus(
        WorkflowStatus state, C context, EventMessage<?> eventMessage, ProcessingContext processingContext) {
    var member = membersByStatus.get(state);
    if (member == null) return;
    var contextWithResources = processingContext
            .withResource(WORKFLOW_CONTEXT_RESOURCE_KEY, context)
            .withResource(WORKFLOW_STATUS_RESOURCE_KEY, state)
            .withResource(INSTANCE_RESOURCE_KEY, instance);
    var future = inspector.chainedInterceptor(instance.getClass())
                          .handle(eventMessage, contextWithResources, instance, member)
                          .first().asCompletableFuture();
    FutureResolver.resolve(processingContext, future);
}
```

`WorkflowReflectionUtils.invoke` stays exactly as-is and remains used **only** by the 2-arg legacy
fallback (confirmed by grep: its only two call sites today are both being replaced; after this
change its only caller is this fallback — no dead code).

**Exception translation** moves off `WorkflowReflectionUtils.invoke`'s bespoke wrapping for this
path: `MethodInvokingMessageHandlingMember.handle` already unwraps `InvocationTargetException.getCause()`
into a failed `MessageStream`, and `FutureResolver.resolve` (this module's existing bounded-blocking
convention — already used by `WorkflowLifecycleControlDelegate`, `ExecuteDelegate`, `VersionDelegate`,
`PayloadDelegate`, `WaitForDelegate` — default 30s timeout, never a raw `.join()`/`.get()`) unwraps
the future's exceptional completion and rethrows the original cause.

---

## Part C — The `@Workflow` body

### C.1 Wire into `AutoDetectingWorkflowBuilder.mapToWorkflowConfiguration`

File: `.../configuration/AutoDetectingWorkflowBuilder.java`, lines 104-121.

`WorkflowDefinition<C>`'s **public contract stays untouched** — still `@FunctionalInterface extends
Consumer<T>`. Replace the hand-rolled parameter loop + `WorkflowReflectionUtils.invoke`:

```java
var member = inspector.getUniqueHandlers(instance.getClass(), CommandMessage.class).stream()
        .filter(m -> m.unwrap(Executable.class)
                      .map(e -> AnnotationUtils.findAnnotationAttributes(e, Workflow.class).isPresent())
                      .orElse(false))
        .findFirst().orElseThrow(/* the one @Workflow method already located by workflowMethod */);

WorkflowDefinition<C> workflowDefinition = workflowContext -> {
    var command = new GenericCommandMessage(messageType, workflowContext);  // messageType already
                                                                             // computed above from
                                                                             // workflowName/workflowVersion
    var contextWithResources = /* the body's own dedicated ProcessingContext — confirm exact
                                   access at implementation time, see open item below */
            .withResource(WORKFLOW_CONTEXT_RESOURCE_KEY, workflowContext)
            .withResource(INSTANCE_RESOURCE_KEY, instance);
    var future = inspector.chainedInterceptor(instance.getClass())
                          .handle(command, contextWithResources, instance, member)
                          .first().asCompletableFuture();
    FutureResolver.resolve(contextWithResources, future);
};
```

Reuses the `MessageType` already computed in `mapToWorkflowConfiguration` from
`workflowName`/`workflowVersion` (today used only for `validateWorkflowDefinition`) — no new naming
convention invented. No changes needed to `SimpleWorkflowExecution`/`WorkflowConfigurationRegistry`;
they already treat `WorkflowDefinition<C>` as an opaque `Consumer<C>`.

**Open implementation-time item:** confirm exactly how the lambda obtains the body's own dedicated,
non-transactional `ProcessingContext` (from `workflowBodyUnitOfWorkFactory`, per
`SimpleWorkflowExecution.executeWorkflow`) — via `ProcessingContext.current()` inside the body's own
thread, or threaded in explicitly. Not a design fork, just needs one more read of
`SimpleWorkflowExecution`/`ProcessingContextUtils.executeWithResultInSeparateThread` before coding.

### C.2 Why `CommandMessage`, and what it costs vs. `EventMessage`

`HandlerTimeoutHandlerEnhancerDefinition` gates on `canHandleMessageType(EventMessage.class | CommandMessage.class | QueryMessage.class)`
— all three accepted, so the body keeps timeout support. `TracingHandlerEnhancerDefinition` wraps
unconditionally regardless of message type, so the body keeps tracing. The body loses
`ReplayAwareMessageHandlerWrapper` (gated on `EventMessage.class` only) — correct, since the body is
never invoked from a real AF5 event-processor replay, so that enhancer was never meaningful for it.

### C.3 Exception translation for the body

Same convention as B.3: `MethodInvokingMessageHandlingMember.handle` unwraps reflection exceptions;
`FutureResolver.resolve` unwraps the future and rethrows the original cause.
`WorkflowReflectionUtils.invoke` is no longer called from this path at all.

---

## Shared parameter resolution

New `@Internal` `ParameterResolverFactory`, `WorkflowMethodParameterResolverFactory`, in
`io.axoniq.framework.workflow.configuration`, next to `WorkflowStateParameterResolverFactory` — one
factory serves **both** call sites, since a `@Workflow` body method and a lifecycle-handler method
draw from the same pool of recognizable parameter shapes. Its `createInstance(Method, Parameter[], int)`
recognizes, by declared parameter type:

- assignable to `WorkflowContext` → resolver reads `WORKFLOW_CONTEXT_RESOURCE_KEY`.
- assignable to `WorkflowStatus` → resolver reads `WORKFLOW_STATUS_RESOURCE_KEY` (only ever
  populated for lifecycle-handler invocations; naturally absent for body invocations).
- assignable from the method's declaring class → resolver reads `INSTANCE_RESOURCE_KEY` (mirrors
  today's `paramType.isInstance(instance)` fallback).
- a "wrapper" type per the existing `AutoDetectionUtils.isWrapper`/`wrapIfPossible` convention →
  resolver reads the `WorkflowContext` resource and calls `AutoDetectionUtils.wrapIfPossible(type, ctx)`
  (this helper is kept — its constructor-probing logic is still correct once we know a wrapper is
  needed; only the *scanning* that decided a method was eligible moves to the `HandlerDefinition`
  above).
- otherwise returns `null` (falls through to whatever other `ParameterResolverFactory`s are
  composed in — e.g. `Metadata`/`@MetadataValue` become usable on both kinds of workflow-annotated
  methods as a natural side effect).

Register via the existing "compose, don't replace" idiom already used in
`WorkflowConfigurationDefaults.enhance(...)` next to `registerWorkflowStateParameterResolverFactory`:
```java
ParameterResolverFactoryUtils.registerToComponentRegistry(
        componentRegistry, cfg -> new WorkflowMethodParameterResolverFactory());
```

This also directly closes the parameter-resolution half of companion issue #413 for **both**
workflow-annotated method kinds, as a side effect (the hand-rolled `wrapIfPossible` constructor-probe
*decision* and the `// FIXME using parameter resolver` in `AutoDetectionUtils.workflowMethods` both
go away for real parameter binding).

---

## What this design does not need

- No new public SPI of any kind (`WorkflowDefinitionEnhancerDefinition` and friends from the first
  draft; the hand-rolled `WorkflowHandlingMemberFactory` helper from the second draft).
- No decision about whether `DecoratorDefinition` fits (it doesn't, and doesn't need to — moot).
- No bespoke annotation-scanning code beyond one small `HandlerDefinition` — everything else
  (hierarchy walking, enhancer composition, interceptor-chain wiring) is Axon Framework's own,
  already-proven machinery.

---

## Part D — Tests

### Update existing tests
- `AutoDetectionUtilsTest.java`, `AutoDetectionLifecycleListenerTest.java` — rewritten around the
  `AnnotatedHandlerInspector`-based construction; existing 2-arg `onWorkflowStatus(...)` calls now
  exercise the documented legacy fallback path — keep them, add new cases for the 4-arg path.
- `EventSourcedWorkflowStateTest.java` — direct `setStatus(...)` calls move to the new 5-arg
  signature; needs a test `EventMessage` (reuse `EventTestUtils`) and whatever `ProcessingContext`
  test double this test class's siblings already use.
- `WorkflowConfigurationDefaultsTest.java` — assert `WorkflowMethodParameterResolverFactory` is
  present/composed after `enhance(...)`.
- `WorkflowModuleTest.java`, `FullConfigurationTest.java` — re-run as regression coverage for the
  config-time wiring; no behavior assertions expected to change.
- `WorkflowReflectionUtilsTest.java` — no change expected; keep as regression coverage for the one
  remaining legacy-fallback caller.

### New tests
1. Register a stub `HandlerEnhancerDefinition` that records invocation; drive a status transition
   through `EventSourcedWorkflowState.evolve(message, ctx, true)`; assert the stub observed the call
   and the annotated method actually ran.
2. Same, for the **body**: register a stub `HandlerEnhancerDefinition`, run a workflow end-to-end
   (`FullConfigurationTest`/`WorkflowModuleTest` harness style), assert the stub wrapped the body's
   invocation too — proof one enhancer chain now covers both seams.
3. A lifecycle handler *and* a body method, each with a `@MessageHandlerInterceptor` method declared
   on the same class, both actually get intercepted — proof the `AnnotatedMessageHandlingMemberDefinition`
   delegate correctly wires the interceptor chain for both.
4. A lifecycle handler *and* a body method, each annotated with a handler-timeout annotation, are
   both actually bounded by `HandlerTimeoutHandlerEnhancerDefinition`.
5. Direct unit test of `WorkflowMethodParameterResolverFactory`: resolves `WorkflowContext`,
   `WorkflowStatus`, declaring-instance, and wrapper-typed parameters for both a
   lifecycle-handler-shaped and a body-shaped method; returns `null` for anything else.
6. Legacy 2-arg `onWorkflowStatus` implementations still fire correctly via the interface's default
   delegation when driven through the new 4-arg call path.
7. Replay-safety regression: drive `evolve(message, ctx, false)` with a registered
   `HandlerEnhancerDefinition`/lifecycle handler present and assert it is **not** invoked.
8. `ReplayAwareMessageHandlerWrapper` suppresses a lifecycle handler during a genuine replay, but a
   body method with the same annotation is unaffected — proves the `EventMessage`/`CommandMessage`
   split does real, intentional work.
9. A method on the workflow class with a real `@CommandHandler`/`@EventHandler` (unrelated to
   workflow annotations) is correctly ignored by the workflow construction path, proving the
   filter-back-out after `getUniqueHandlers(...)` works as intended.

All new tests follow this repo's conventions: JUnit 5, AssertJ, `// given / when / then`, `@Nested`
grouping, `EventTestUtils` for sample events, no Mockito unless a `spy()` is genuinely needed per the
repo's mocking rule (state-based assertions preferred throughout).

### Docs
- `docs/reference-guide/modules/workflows/pages/workflow-lifecycle.adoc` — note that annotation-based
  lifecycle listeners now execute through the standard Axon Framework handler pipeline (tracing,
  handler timeouts, `@MessageHandlerInterceptor`, replay-awareness, and any registered
  `HandlerEnhancerDefinition` apply automatically), and that programmatic listeners can opt into the
  same behavior via the new 4-arg `onWorkflowStatus` overload. Flag separately (not part of this
  fix): this page's existing code sample uses annotation names (`@OnSuccess`/`@OnFailure`/etc.) that
  don't exist in source — a pre-existing, unrelated doc bug worth a follow-up.
- Wherever `@Workflow` body semantics are documented — a short note that the body now also
  participates in the same handler pipeline (tracing/timeouts/interceptors), with the `EventMessage`
  vs `CommandMessage` distinction explained in plain terms ("the body responds to a resume request,
  not to the original triggering event").

---

## Verification

- `./mvnw test -pl workflow/axoniq-workflow-engine -am` for the unit tests above.
- `./mvnw -pl workflow/axoniq-workflow-engine -am clean verify` for the full module build
  (checkstyle/javadoc rules — fragment-style `@param`/`@return`, `@link` everywhere, `@Internal`
  justification comments).
- `./mvnw clean verify` at the repo root, since `axoniq-workflow-dsl`, `axoniq-workflow-spring-boot`,
  and the Kotlin variants all depend on `axoniq-workflow-engine` and touch
  `AutoDetectingWorkflowBuilder`/`AutoDetectionUtils` indirectly.
- Manually confirm (via a quick throwaway test or example app run) that a registered
  `TracingHandlerEnhancerDefinition`-style span actually appears around both a lifecycle handler
  invocation and a body invocation, closing the loop on the issue's original motivating complaint.
