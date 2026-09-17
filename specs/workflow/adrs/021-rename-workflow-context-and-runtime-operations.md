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

The author-facing type is named `WorkflowContext`.

The runtime primitive-operation surface is named `WorkflowExecutionOperations`.

Names referring to each surface follow the same distinction: author-facing parameters and variables use
`workflowContext`; runtime primitive-operation parameters and variables use `workflowExecutionOperations`. The
`WorkflowExecution` accessor for the latter is named `workflowExecutionOperations()`.

## Consequences

- Workflow bodies receive a type whose name expresses their author-facing role.
- Runtime delegates use a name that describes their command and execution-metadata responsibilities.
- The separation, delegation model, execution behavior, persisted events, and `WorkflowStepResult` semantics decided
  in ADR-020 remain unchanged.

This ADR changes terminology and API names only. It does not supersede or deprecate ADR-020.
