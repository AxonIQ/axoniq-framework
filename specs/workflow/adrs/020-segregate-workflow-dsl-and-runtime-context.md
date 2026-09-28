# ADR-020: Segregate Workflow DSL and Runtime Context

- Status: Accepted
- Date: 2026-09-17

## Context

[ADR-001](./001-command-mode-primitives.md) separates author-facing step definitions from runtime primitive commands.
`AbstractDSLWorkflowContext` currently implements both `WorkflowDSL` and `WorkflowContext`, however. As a result,
DSL authors inherit primitive methods accepting runtime commands, and context factories must create an object that is
simultaneously a DSL adapter and a runtime context.

## Decision

`WorkflowDSL` and `WorkflowContext` are separate interfaces.

- Workflow definitions and their factories use a type extending `WorkflowDSL`.
- Runtime delegates use only `WorkflowContext` and primitive commands.
- `AbstractDSLWorkflowContext` implements `WorkflowDSL` only. It translates step definitions to primitive commands and
  delegates them to an internal runtime `WorkflowContext`.
- `WorkflowContextDelegation` is the runtime context. It retains the DSL object only to invoke the configured workflow
  definition, and passes itself to runtime delegates.
- `WorkflowStepResult` remains the common result type for DSL and runtime operations.

The execution factory uses an internal bridge to obtain a `WorkflowExecution` from a DSL context. This bridge does not
make the DSL context a `WorkflowContext`.

## Consequences

- DSL consumers cannot accidentally call primitive-command runtime APIs.
- Runtime code no longer depends on a DSL implementation being a `WorkflowContext`.
- Context factories, workflow definitions, configuration builders, Spring discovery, and test fixtures use the DSL
  context type instead of the runtime context type.
- Execution behavior, persisted events, and `WorkflowStepResult` semantics remain unchanged.
