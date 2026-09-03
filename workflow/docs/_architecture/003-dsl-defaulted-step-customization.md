# ADR-003: Canonical And Convenience DSL Layers

- Status: Accepted
- Date: 2026-05-06

## Context

ADR-002 defines the step-definition layer that bridges author-facing workflow code to the runtime.
On top of that layer, the project exposes authoring APIs with different ergonomics for Java and Kotlin.
Those APIs need a stable architectural boundary so convenience helpers can evolve without creating alternate execution paths.

## Decision

`BaseWorkflowContext` is the canonical Java DSL layer.
It owns default step-definition creation, applies Java-side customization, and delegates to the
definition-based API in `AbstractDSLWorkflowContext`.

- `SimpleWorkflowContext` is a thin Java convenience layer on top of `BaseWorkflowContext`.
  It may add readability, typed, or defaulted overloads for common cases, but those overloads must
  delegate through `BaseWorkflowContext`.
- Kotlin follows the same pattern with different types:
  `WorkflowKontext` is the execution-facing DSL context built on `AbstractDSLWorkflowContext`,
  while `Kontext` is the author-facing wrapper that adds Kotlin-first overloads and default
  parameters.

The concrete helper inventory is intentionally not fixed in this ADR.
Reference documentation should describe the currently exposed overloads and examples.

## Rules

- Defaults belong in the canonical DSL layers, not in runtime command handling.
- Customization operates on DSL definitions, not on runtime commands.
- Convenience APIs must remain adapters above the canonical definition-based path.
- New overloads are acceptable when they improve author ergonomics without creating parallel runtime semantics.
- Concrete helper names, defaults, and examples belong in the user documentation, not in this ADR.

## Consequences

- The definition layer remains the single bridge from DSL authoring to runtime commands.
- Java and Kotlin can offer different authoring ergonomics without diverging in execution semantics.
- Convenience methods can be added, renamed, or removed with documentation updates, while the
  architectural contract stays stable.
