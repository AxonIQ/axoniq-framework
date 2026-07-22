# ADR-013: Cancellation and termination semantics

- Name: Cancellation and termination semantics
- Status: Accepted
- Date: 2026-07-21

## Context

A workflow can terminate in four ways — completion, failure ([`ctx.fail`](./001-command-mode-primitives.md)),
cancellation (`ctx.cancel`), and timeout — and a single running step can be cancelled independently
(`ctx.cancelStep`, `WorkflowStepResult.cancel()`, or externally). These are two different concerns that were
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

**Whole-workflow termination** (completion, fail, cancel, timeout — in-body or external) publishes only the
**workflow-level** terminal event (`<workflow>:COMPLETED` / `FAILED` / `CANCELLED` / `TIMED_OUT`). Running steps
are **interrupted**, not individually cancelled: their futures are completed with a non-cancellation cause and
the pending task queue is discarded. No per-step terminal record is published; a step that was running is left
in its last recorded state (`STARTED`) in the event log. Because the queue is discarded rather than drained, a
queued retry attempt cannot launch, so no cross-task "terminating" flag or ordering drain is needed; a retry
launch additionally gates on `workflowStatus().isTerminal()`.

**Single-step cancellation** (`ctx.cancelStep`, `WorkflowStepResult.cancel()`, and the external
`WorkflowManager.cancelStep` / `cancelAllRunningSteps`) still publishes a `<step>:CANCELLED` record and lets the
body catch `StepCancellationException` and compensate. The record is authored **on the workflow control thread**
(the single consumer of the task queue): in-body callers author it synchronously; external callers enqueue the
work onto the control thread and never drive workflow logic from the caller thread. The not-terminal check and
the publish are therefore atomic with respect to concurrent step completion — first-writer-wins, no race.

External management is exposed only through `WorkflowManager` (`cancel`, `cancelStep`, `cancelAllRunningSteps`),
never by mutating a live execution from outside. External operations are cooperative and asynchronous: they
return a result describing what was requested without blocking on the terminal state, and an uncaught
`StepCancellationException` propagates and wedges the instance non-terminally exactly like any other uncaught
exception (the caller's body is responsible for catching it).

### How a step's fate is decided (one mechanism, two causes)

Both paths tear a running step down the same way — by completing its `CompletableFuture` exceptionally. The
**type of the cause** is the switch. Each step's completion handler asks `isCancellation(cause)`:

- a **cancellation cause** (`StepCancellationException` / `WorkflowCancelledException` / `WorkflowFailedException`)
  → the handler publishes a `<step>:CANCELLED` record;
- **any other cause** (specifically an `InterruptedException`) → the handler publishes nothing and merely
  deregisters the step.

So the two paths pick different causes on purpose:

- **Whole-workflow termination** completes the futures with an `InterruptedException` → silent teardown, no
  per-step record (only the workflow-level terminal event is written). Completing a step's future is not a
  force-interrupt: a user `execute` action already running on the executor runs to completion and its late
  result is simply discarded.
- **Single-step cancellation** completes with a `StepCancellationException` → a `<step>:CANCELLED` record is
  written, which the body observes and can catch to compensate.
