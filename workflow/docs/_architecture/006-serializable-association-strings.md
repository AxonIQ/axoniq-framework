# ADR-006: Serializable Association Strings

- Status: Accepted
- Date: 2026-07-07

## Context

`waitForEvent` for running instances currently reaches execution as an `EventCondition` whose
match portion is often an arbitrary `BiPredicate<EventMessage, ProcessingContext>`. In practice,
the simple DSL builds that matcher from association helpers and `Associations`, and
`EventWaitConditions.evaluateAndApply(...)` only observes whether the predicate returns `true`.

That is enough for live matching, but it loses the association intent. Running wait conditions are
not serializable, not inspectable, and cannot share one canonical format with
`@Workflow(startOnConditions = ...)` or a future configuration repository.

The project already has two building blocks worth preserving:

- string-based associations in `@Workflow(startOnConditions = ...)`
- `ValueComparisonOperatorRegistry` as the SPI for comparison operators

The current string form is also too narrow. It is effectively payload-only and implicit
(`status=vip`). That does not leave room for future qualifiers such as `metadata:key=value` or
`message:timestamp>2026-01-01T12:00:00Z`.

The primary requirement is to make running-instance wait conditions use a serializable association
representation. Reuse in configuration is optional.

## Decision

Association-based wait conditions use a canonical qualified string form at the execution boundary.
Callers do not register arbitrary match lambdas for those waits.

Canonical form:

- `<source>:<path><operator><value>`
- `payload:paymentReference=123`
- `metadata:tenantId=acme`
- `message:timestamp>2026-01-01T12:00:00Z`

Rules:

- `source` identifies where the left-hand value comes from. `payload` is the first required
  qualifier.
- `path` is source-specific lookup text, such as a payload property or metadata key. This maps to a value retriever.
- `operator` is resolved through `ValueComparisonOperatorRegistry`.
- `value` is stored in serialized text form.

DSL helpers become serializers first, predicate builders second. Author-facing helpers such as
`payloadProperty("foo")` plus `equals("bar")` produce the canonical string `payload:foo=bar`
(or a typed value object whose canonical form is that string). The simple wait-for-event DSL uses
that serialized representation for running instances.

Runtime evaluation may still use predicates internally, but those predicates are derived from the
canonical association string through parsing plus registered qualifier handlers and registered
operators. The runtime stores and describes the serial form, not caller-supplied executable code.

The same qualified string format may be reused by `@Workflow(startOnConditions = ...)` and by any
future configuration repository, but those configuration paths are secondary consumers. Running
instance wait registration is the primary driver.

## Scope for now

This ADR does not require the full association text DSL in one step.

The initial implementation only needs:

- a canonical qualified payload form such as `payload:id=123`
- DSL helpers that emit that form for wait-for-event usage
- runtime parsing and evaluation that turn the string back into a matcher
- preservation of the existing operator SPI

Future work may add more qualifiers such as `metadata` and `message`, richer operators, and
source-specific parsing rules without changing the canonical shape.

Configuration-based start conditions use the same qualified form. The qualified string is the
canonical representation exposed by APIs and documentation.

## Consequences

- Wait conditions for running instances become serializable and inspectable.
- The DSL gets one primary association representation instead of treating strings as a
  configuration-only special case.
- Association parsing must distinguish qualifier, path, operator, and literal value instead of
  assuming payload-only lookup.
- The operator SPI remains valid and becomes part of the canonical association model.
- Configuration-based start conditions reuse the same canonical format.
- Arbitrary `BiPredicate<EventMessage, ProcessingContext>` matching is no longer the primary API
  for association-based waits.
