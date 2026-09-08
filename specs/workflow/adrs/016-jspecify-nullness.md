# ADR-016: JSpecify Nullness by Default

- Name: JSpecify Nullness by Default
- Status: Accepted
- Date: 2026-08-28

## Context

The project currently mixes Jakarta, Spring, and JSpecify nullness annotations. Most declarations explicitly repeat
that a value is non-null, while the few nullable exceptions are harder to identify. Multiple annotation systems also
give tools and API users inconsistent nullness contracts.

## Decision

Use JSpecify as the project's only nullness annotation system. Every Java package under a source set is annotated with
`@NullMarked` in `package-info.java`, making unannotated types non-null by default. Use
`org.jspecify.annotations.Nullable` only where a type is permitted to be null. Do not use explicit `@NonNull`.

Declare JSpecify as a direct project dependency. Checkstyle rejects Jakarta, Spring, JetBrains, and explicit JSpecify
non-null annotations, preventing reintroduction of a competing or redundant convention.

## Consequences

Nullness contracts become shorter and uniform across production and test code. New packages must include a
`@NullMarked` package descriptor. Nullable declarations remain explicit, which makes API exceptions visible during
review. Tools that do not yet consume JSpecify annotations still see unchanged runtime behavior because the
annotations are descriptive only.
