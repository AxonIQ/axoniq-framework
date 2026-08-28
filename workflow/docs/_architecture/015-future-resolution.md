# ADR-015: Central Future Resolution

- Name: Central Future Resolution
- Status: Accepted
- Date: 2026-08-28

## Context

Workflow code resolves `CompletableFuture` instances in several places, including durable event publication and the
workflow-body execution thread. These paths currently decide locally how to wait, either using an unconditional
`join()` or a locally chosen timeout. That spreads a runtime policy across unrelated execution classes and makes it
difficult for an application to apply a consistent deadline, failure, or observability policy.

## Decision

Introduce `FutureResolver` as the single extension point for resolving `CompletableFuture` instances.

The resolver is discovered once through Java `ServiceLoader`, following Axon Framework's `IdentifierFactory` pattern.
The discovered resolver is registered in the workflow component registry and waiting paths obtain it from their
`ProcessingContext`. `DefaultFutureResolver` is used when no service is available and preserves the current behaviour
by calling `CompletableFuture.join()`.

All relevant workflow paths that wait for completion delegate to this component. A custom resolver must be thread-safe
and may impose a deadline, translate failures, or record metrics.

## Consequences

Future waiting policy is explicit, consistent, and configurable without changing workflow execution classes. The
default is backwards compatible. Applications that configure a bounded wait must define the resulting failure and
recovery behaviour, because an asynchronous operation may still complete after the resolver has stopped waiting.
