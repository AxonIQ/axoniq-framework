# ADR-013: Cancellation and termination semantics

- Name: Cancellation and termination semantics
- Status: Accepted
- Date: 2026-07-21

## Context

A workflow can terminate in four ways — completion, failure ([`ctx.fail`](./001-command-mode-primitives.md)),
cancellation (`ctx.cancel`), and timeout — and a single running step can be cancelled independently
(`ctx.cancelStep` or `WorkflowStepResult.cancel()`). These are two different concerns that were
previously handled by one path: every whole-workflow terminal first cancelled each running step individually,
publishing a `<step>:CANCELLED` record per step before the workflow terminal record.

That per-step-terminal-before-workflow-terminal ordering forced a chain of machinery: draining the task queue
during termination to flush the step events in order, a `terminating` flag so a retry attempt drained during
that window would not launch, and careful gating so the drain did not run doomed work. The ordering also could
not be made deterministic without that shared state, and the per-step records duplicated information the
workflow terminal already carries.

Single-step cancellation is genuinely different: it must let the workflow body observe the cancellation
(`StepCancellationException`) and run compensation, so it needs the per-step record and must be delivered on the
workflow's own control thread.

## Decision

Separate the two concerns.

**Whole-workflow termination** (completion, fail, cancel, timeout) publishes only the
**workflow-level** terminal event (`<workflow>:COMPLETED` / `FAILED` / `CANCELLED` / `TIMED_OUT`). Running steps
are **interrupted**, not individually cancelled: their futures are completed with a non-cancellation cause and
the pending task queue is discarded. No per-step terminal record is published; a step that was running is left
in its last recorded state (`STARTED`) in the event log. Because the queue is discarded rather than drained, a
queued retry attempt cannot launch, so no cross-task "terminating" flag or ordering drain is needed; a retry
launch additionally gates on `workflowStatus().isTerminal()`.

**Single-step cancellation** (`ctx.cancelStep` and `WorkflowStepResult.cancel()`) still publishes a
`<step>:CANCELLED` record and lets the body catch `StepCancellationException` and compensate. The record is
authored on the workflow control thread, the single consumer of the task queue. The not-terminal check and publish
are therefore atomic with respect to concurrent step completion: first writer wins.

### How a step's fate is decided (one mechanism, two causes)

Both paths tear a running step down the same way — by completing its `CompletableFuture` exceptionally. The
**type of the cause** is the switch. Each step's completion handler asks `isCancellation(cause)`:

- a **cancellation cause** (`StepCancellationException` / `WorkflowCancelledException` / `WorkflowFailedException`)
  → the handler publishes a `<step>:CANCELLED` record;
- **any other cause** (specifically an `InterruptedException`) → the handler publishes nothing and merely
  deregisters the step.

So the two paths pick different causes on purpose:

- **Whole-workflow termination** completes the futures with a `StepInterruptedException` → silent teardown, no
  per-step record (only the workflow-level terminal event is written). Completing a step's future is not a
  force-interrupt: a user `execute` action already running on the executor runs to completion and its late
  result is simply discarded.
- **Single-step cancellation** completes with a `StepCancellationException` → a `<step>:CANCELLED` record is
  written, which the body observes and can catch to compensate.

If the workflow body itself is blocked on the interrupted step (for example inside `ctx.awaitExecute(...)`), the
same `StepInterruptedException` unblocks it, so the body can catch it (or its parent `StepFailedException`) for
compensation or cleanup, exactly like it would `StepCancellationException` for a single-step cancel. Catching it
has no bearing on what is durable: the step's last recorded event-log state stays `STARTED` either way.

### Parked-step timer lifecycle

Waiting for an event and waiting for a retry backoff are both parked-step phases. A parked step has one owner that
is registered with `RunningSteps` before its timer can become eligible. The owner keeps the completion future and
the `ScheduledTask` handle together, and settles the phase exactly once.

Every terminal path settles that same owner: the awaited event arrives, the timer expires, the step is cancelled,
the workflow reaches a terminal state, or the engine stops for shutdown. Settling it removes the running-step
registration and cancels the pending timer when it has not already fired. This prevents a completed or cancelled
parked step from retaining a delayed task, its workflow execution, and its event-wait condition until the original
deadline. Registration and cleanup must be ordered so a future that has already completed cannot be left in
`RunningSteps`.

The scheduler is a lifecycle-owned, configurable dependency of the workflow module or engine. It is not a static
process-wide executor hidden in `DefaultWorkflowScheduler`. A timer callback only signals deadline expiry and
enqueues the workflow continuation on the workflow control queue. It must not execute step logic, publish events,
or otherwise block on the timer executor. This keeps timer delivery separate from workflow work and avoids one
slow callback delaying unrelated workflow deadlines.

## Consequences

- `RunningSteps` tracks a cancellable parked-step owner rather than only a bare completion future where a timer is
  involved.
- Wait and retry implementations share the same parked-step lifecycle and cancellation rules.
- Timer capacity is selected at engine or module configuration time. It is not an arbitrary constant in a static
  scheduler.
- Tests cover event completion, step cancellation, whole-workflow termination, shutdown, and an immediately due
  timer, asserting that the scheduled task and running-step registration are removed in every case.
