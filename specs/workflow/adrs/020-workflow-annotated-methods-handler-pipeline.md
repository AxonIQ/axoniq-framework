# ADR-020: Workflow-Annotated Methods Join the Handler Pipeline

- Status: Accepted
- Date: 2026-09-22

## Context

`@Workflow` bodies and `@WorkflowStatusChangedHandler` lifecycle listeners were invoked by raw
`Method.invoke` reflection (`WorkflowReflectionUtils.invoke`), bypassing the Axon Framework handler
pipeline entirely. Even though the annotations look like ordinary Axon Framework handler
annotations, there was no `MessageHandlingMember`, no `HandlerDefinition`, and no seam for a
`HandlerEnhancerDefinition` to wrap them — so tracing, `@MessageHandlerInterceptor`, handler
timeouts, and replay-awareness were all unavailable to workflow authors (issue #412).

Parameter binding had the same gap: a hand-rolled type-matching loop stood in for
`ParameterResolverFactory`, so workflow methods were the only annotated methods in the stack that
could not receive anything the framework resolves for every other handler. An unresolvable
parameter silently bound to `null` instead of failing at configuration time (issue #413).

## Decision

Recognize `@Workflow` and `@WorkflowStatusChangedHandler` methods through two dedicated
`HandlerDefinition`s, `AnnotatedWorkflowHandlerDefinition` and
`AnnotatedWorkflowStatusChangedHandlerDefinition`, feeding one `AnnotatedHandlerInspector` built
once per workflow instance and composed with the configuration's `HandlerEnhancerDefinition` chain
via `MultiHandlerDefinition` — mirroring how `AnnotatedCommandHandlingComponent` itself is built.
This gets `@MessageHandlerInterceptor` chain-wiring, tracing, and handler timeouts for free,
matching how the rest of the framework works instead of approximating it.

The `@Workflow` body's public seam stays untouched: `WorkflowDefinition<C>` remains a plain
`Consumer<C>`. Internally, each invocation is bridged into the handler pipeline by synthesizing a
`WorkflowTriggerMessage` — a dedicated `Message` subtype carrying the `WorkflowContext` as
its payload, never dispatched on a real bus — and routing it through the enhanced
`MessageHandlingMember`. The resulting future is resolved promptly (with a timeout) rather than
kept open for the workflow's lifetime, so the body keeps its "handled once per invocation, no
message retained across restarts" semantics while still gaining enhancer/interceptor coverage.
Lifecycle listeners use the *real* `EventMessage` that drove the status transition instead, since
that is genuinely a reaction to something that happened and is meaningfully replayable.

`AnnotatedWorkflowHandlerDefinition` is classpath-discovered (registered globally via
`META-INF/services`), because it recognizes methods narrowly by the `@Workflow` annotation itself —
safe anywhere it might be scanned. `AnnotatedWorkflowStatusChangedHandlerDefinition` is deliberately
*not* globally registered: it recognizes methods by the generic `EventMessage` type, so registering
it globally would risk `@WorkflowStatusChangedHandler` methods being misclassified as ordinary event
handlers wherever an unrelated `AnnotatedHandlerInspector` scan encounters them. It is instead wired
in by hand, as one of the delegates `AutoDetectingWorkflowBuilder` passes into its own local
`MultiHandlerDefinition`.

Parameter resolution moves to a single `WorkflowMethodParameterResolverFactory` serving both call
sites — a `@Workflow` body and a lifecycle listener draw from the same pool of recognizable
parameter shapes (`WorkflowContext`, `WorkflowStatus`, the declaring instance, and any type with a
constructor accepting a `WorkflowContext`). Because this factory is registered application-wide, it
gates every call through an `isWorkflowAnnotated` check first, so it cannot resolve parameters on
unrelated handlers — most notably an `Object`-typed parameter, which is trivially assignable from
any declaring class.

Every future produced by the internal handler-pipeline bridge is resolved through the existing
`FutureResolver` convention ([ADR-015](./015-future-resolution.md)) rather than a raw blocking call,
so a slow enhancer or interceptor cannot leak a thread indefinitely.

## Consequences

- Workflow-annotated methods benefit from the same cross-cutting concerns (tracing, timeouts,
  interceptors) as every other Axon Framework handler, with no extra setup for workflow authors.
- `AutoDetectionUtils`'s own scanning code shrinks to attribute extraction; eligibility and
  construction are the framework's own, already-proven machinery instead of a bespoke
  approximation of it.
- An unresolvable parameter now fails workflow configuration at startup, naming the method, instead
  of silently binding to `null` and failing later inside a running workflow.
- The registration asymmetry between the two `HandlerDefinition`s is deliberate, but the safety
  argument for `AnnotatedWorkflowHandlerDefinition`'s own global registration currently lives on
  `AutoDetectingWorkflowBuilder`'s Javadoc rather than on the class itself — worth cross-referencing
  from there directly.
- `AutoDetectionUtils` still runs its own hierarchy scan (`ReflectionUtils.methodsOf`) to find
  candidate methods, separately from `AnnotatedHandlerInspector.getUniqueHandlers()`'s own
  override-deduplicating view. The two can disagree when a workflow class overrides and re-annotates
  a `@Workflow` or `@WorkflowStatusChangedHandler` method on both the base method and the override,
  raising a configuration-time error. Deriving candidates directly from `getUniqueHandlers()` instead
  would close this gap; tracked as a follow-up, not resolved by this decision.
- The public workflow vocabulary (`WorkflowContext`, `WorkflowDefinition`,
  `WorkflowStatusChangeListener`) stays free of Axon Framework `Message`/`MessageHandlingMember`
  types in its own signatures; the framework-facing machinery lives entirely in `@Internal` adapter
  classes under `configuration`.
