# ADR-022: Workflow-Annotated Methods Join the Handler Pipeline

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
once per workflow instance. Both `HandlerDefinition`s are classpath-discovered (registered via
`META-INF/services`), so `AutoDetectingWorkflowBuilder` simply asks the `Configuration` for its
composed `HandlerDefinition` component — it already recognizes both `@Workflow` bodies and
`@WorkflowStatusChangedHandler` lifecycle methods, on top of the framework's default recognizers
(including `AnnotatedMessageHandlingMemberDefinition`, needed so `@MessageHandlerInterceptor`
methods declared on the same class are recognized and chained). Axon Framework's own
`HandlerDefinitionUtils#registerToComponentRegistry` always composes every registered
`HandlerDefinition` with the current `HandlerEnhancerDefinition` component through
`MultiHandlerDefinition`, so `AutoDetectingWorkflowBuilder` does not assemble that composition
itself — doing so would apply the enhancer chain twice. This mirrors how
`AnnotatedCommandHandlingComponent` itself is built, and gets `@MessageHandlerInterceptor`
chain-wiring, tracing, and handler timeouts for free, matching how the rest of the framework works
instead of approximating it.

Both call sites keep their public seam untouched — `WorkflowDefinition<C>` remains a plain
`Consumer<C>`, and `WorkflowStatusChangeListener`'s single method never mentions any Axon Framework
`Message` type. Internally, each invocation is bridged into the handler pipeline by synthesizing a
dedicated `Message` subtype — never dispatched on a real bus — and routing it through the enhanced
`MessageHandlingMember`:

- A `@Workflow` body receives a `WorkflowTriggerMessage`, carrying the `WorkflowContext` as its
  payload.
- A `@WorkflowStatusChangedHandler` lifecycle method receives a `WorkflowStatusChangeMessage`,
  carrying the `WorkflowStatus` the workflow transitioned into as its payload. This is a fully
  synthetic notification, not the real `EventMessage` that drove the status transition — the
  listener only ever cares about the resulting `WorkflowStatus`, so reusing the real triggering
  event would mix an artificial dispatch use case into `EventMessage`'s vocabulary for no benefit.
  A `MessageType` is synthesized once per detected lifecycle method, from its declaring class and
  method name, and reused for every invocation of that listener.

In both cases the resulting future is resolved promptly (with a timeout) through the existing
`FutureResolver` convention ([ADR-015](./015-future-resolution.md)) rather than kept open or
blocked on, so a slow enhancer or interceptor cannot leak a thread indefinitely, and neither call
site keeps a message retained across restarts.

Because both `HandlerDefinition`s key off narrow, synthetic message types (`WorkflowTriggerMessage`,
`WorkflowStatusChangeMessage`) instead of a generic Axon Framework message type, classpath-discovering
both is safe: `MethodInvokingMessageHandlingMember#canHandleMessageType` checks
`messageType.isAssignableFrom(given)`, so an unrelated `AnnotatedHandlerInspector` scan (for
example, over a class also carrying real `@EventHandler` methods) can never mistake a
`@WorkflowStatusChangedHandler` method for an ordinary event handler — its declared message type
simply does not match `EventMessage`.

Parameter resolution moves to a single `WorkflowMethodParameterResolverFactory` serving both call
sites — a `@Workflow` body and a lifecycle listener draw from the same pool of recognizable
parameter shapes (`WorkflowStatus`, `WorkflowContext`, and any type with a constructor accepting a
`WorkflowContext`). Because this factory is registered application-wide, it gates every call
through an `isWorkflowAnnotated` check first, so it cannot resolve parameters on unrelated
handlers — most notably an `Object`-typed parameter, which is trivially assignable from any
declaring class.

## Consequences

- Workflow-annotated methods benefit from the same cross-cutting concerns (tracing, timeouts,
  interceptors) as every other Axon Framework handler, with no extra setup for workflow authors.
- `AutoDetectionUtils`'s own scanning code shrinks to attribute extraction; eligibility and
  construction are the framework's own, already-proven machinery instead of a bespoke
  approximation of it.
- An unresolvable parameter now fails workflow configuration at startup, naming the method, instead
  of silently binding to `null` and failing later inside a running workflow.
- `AutoDetectingWorkflowBuilder` does not hand-assemble a `MultiHandlerDefinition`/
  `HandlerEnhancerDefinition` combination of its own; it trusts the `Configuration`'s own
  `HandlerDefinition` component, which Axon Framework itself already composes with every registered
  enhancer.
- `AutoDetectionUtils` still runs its own hierarchy scan (`ReflectionUtils.methodsOf`) to find
  candidate methods, separately from `AnnotatedHandlerInspector.getUniqueHandlers()`'s own
  override-deduplicating view. The two can disagree when a workflow class overrides and re-annotates
  a `@Workflow` or `@WorkflowStatusChangedHandler` method on both the base method and the override,
  raising a configuration-time error. Deriving candidates directly from `getUniqueHandlers()` instead
  would close this gap; tracked as a follow-up, not resolved by this decision.
- The public workflow vocabulary (`WorkflowContext`, `WorkflowDefinition`,
  `WorkflowStatusChangeListener`) stays entirely free of Axon Framework `Message`/
  `MessageHandlingMember` types in its own signatures; the framework-facing machinery — including
  both synthetic message types and both `HandlerDefinition`s — lives entirely in `@Internal` adapter
  classes under `configuration`.
