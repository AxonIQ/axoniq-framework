# ADR-021: Rename Workflow Context and Runtime Operations

- Status: Accepted
- Date: 2026-09-17

## Context

ADR-020 separates the workflow author's DSL surface from the runtime primitive-operation surface. The names used by
that decision made the author-facing type appear to be a secondary DSL marker (`WorkflowDSL`) while the internal
primitive-operation surface was named `WorkflowContext`.

For a workflow author, the object supplied to a workflow body is naturally the workflow context. The runtime surface,
on the other hand, provides primitive operations and execution metadata; it is not the author's context.

## Decision

The author-facing workflow-body contract is named `WorkflowContext` (renamed from `WorkflowDSL`). It is the primary
type workflow authors use to implement workflow logic.

The internal primitive-operation contract is named `WorkflowExecutionOperations` (renamed from
`WorkflowContext`). The runtime uses it to execute and wait for steps, publish messages, modify payloads, and control
the workflow lifecycle.

Names referring to each surface follow the same distinction: author-facing parameters and variables use
`workflowContext`; runtime primitive-operation parameters and variables use `workflowExecutionOperations`. The
`WorkflowExecution` accessor for the latter is named `workflowExecutionOperations()`.

The supporting types are renamed consistently:

- `AbstractDSLWorkflowContext` becomes `AbstractWorkflowContext`. It is the runtime-backed base implementation of
  the author-facing context and translates workflow step definitions into runtime operations.
- `WorkflowContextDelegation` becomes `WorkflowExecutionOperationsDelegation`. It delegates the runtime
  primitive-operation surface to a running `WorkflowExecution`.
- `DSLAdoptingExecutionFactory` becomes `WorkflowContextAdoptingExecutionFactory`. It adopts an author-facing
  `WorkflowContext` implementation into the execution runtime.

`EventMessageUtils` accepts the new `WorkflowEventPublicationContext`, rather than the full primitive-operation
surface. This read-only context provides the workflow data required to create event messages without coupling event
creation to execution primitives.

## Consequences

- Workflow bodies receive a type whose name expresses their author-facing role.
- Runtime delegates use a name that describes their command and execution-metadata responsibilities.
- Supporting abstractions use names consistent with the surface they implement or adopt.
- Event-message creation depends only on the read-only data it requires.
- The separation, delegation model, execution behavior, persisted events, and `WorkflowStepResult` semantics decided
  in ADR-020 remain unchanged.

This ADR changes terminology, API names, and the event-publication dependency boundary only. It does not supersede or
deprecate ADR-020.
