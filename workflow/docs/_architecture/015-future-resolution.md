# ADR-015: Central Future Resolution

- Name: Central Future Resolution
- Status: Accepted
- Date: 2026-08-28

## Context

Workflow code resolves `CompletableFuture` instances in several places, including durable event publication and the
workflow-body execution thread. These paths need one consistent resolution policy so that a hung dependency does not
hold a workflow thread indefinitely and cascade into a broader outage.

Issue #280 raised a broader architectural concern: most asynchronous operations should be passed to a completion
driver instead of being joined at each call site. This ADR does not introduce that non-blocking completion model.

## Decision

Introduce `FutureResolver` as the single extension point for resolving `CompletableFuture` instances. Its default
implementation uses the same 30-second safety-net timeout as Axon Framework's `FutureUtils.joinAndUnwrap`.

Thirty seconds is sufficient for the synchronous operations workflow code resolves: in-memory work, local database
work, and unit-of-work execution should complete well within that bound. It is also short enough to expose a genuinely
hung operation, such as connection-pool exhaustion, a deadlock, or a network partition, before blocked workflow
threads cascade into a larger outage. A caller that legitimately expects a longer wait must configure that explicitly.

The resolver is discovered through Java `ServiceLoader` while the workflow component registry is initialized. The
selected resolver is registered in that registry, and waiting paths obtain it from their `ProcessingContext`.
`DefaultTimeoutFutureResolver` is used when no service is available. It delegates to
`FutureUtils.joinAndUnwrap(future, timeout)`: a completed future does not incur timeout work, exceptional completion
is rethrown as its original cause, and expiry fails with `TimeoutException`.

All relevant workflow paths that wait for completion delegate to this component. A custom resolver must be thread-safe
and may impose a deadline, translate failures, or record metrics.

### Alternatives considered

`CompletionDriver` was proposed during the #280 discussion. It would accept incomplete operations and resume workflow
progress from completion callbacks, removing synchronous waiting from the workflow call sites. That approach requires
a durable completion contract, explicit ordering and cancellation semantics, and changes to every primitive's control
flow. It is not implemented by this decision.

`FutureResolver` is the narrower decision: it centralizes and bounds the existing blocking behavior, making it
configurable without changing primitive control flow. It therefore does not resolve #280; it makes the current model's
blocking policy explicit while preserving a later path to introduce `CompletionDriver`.

## Consequences

Future waiting policy is explicit, consistent, and configurable without changing workflow execution classes. The
default bounds every synchronous resolution at 30 seconds. Applications that configure a different bound must define
the resulting failure and recovery behaviour, because an asynchronous operation may still complete after resolution
has timed out.

The public workflow surface remains blocking by default. A future `CompletionDriver` decision must change that surface
deliberately rather than treating this resolver as a substitute for asynchronous completion.
