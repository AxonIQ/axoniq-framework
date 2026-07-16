# ADR-011: Workflow Test Mode Execute Overrides And Time Control

- Name: Workflow Test Mode Execute Overrides And Time Control
- Status: Accepted
- Date: 2026-06-08

## Context

The BDD workflow fixture should let tests drive workflow progress without invoking real step side effects. In particular,
fixture methods such as `executeStepReturning(stepName, payload)`, `executeStep(stepName)`, and
`executeStepFailing(stepName, failure)` need to replace the real `execute` step action with test-provided behavior.

The current execute path is:

1. DSL creates an `ExecuteStepDefinition`.
2. `AbstractDSLWorkflowContext` turns it into an `ExecuteCommand`.
3. `WorkflowContextDelegation` delegates to `RetryableExecuteDelegate`.
4. `RetryableExecuteDelegate` delegates to `ExecuteDelegate`.
5. `ExecuteDelegate` marks the step `STARTED`, computes the reduced input payload, and invokes `command.action()`.

This makes `ExecuteDelegate` the narrowest place where production execution can be replaced while preserving all normal
workflow event publication, payload reducers, retry handling, and state transitions.

The fixture also needs deterministic time. Registering a custom `Clock` is necessary, but not sufficient, because
`ExecuteDelegate`, `WaitForDelegate`, and retry handling currently use wall-clock delayed futures. Explicit fixture
methods such as `timePasses(...)` require timeout scheduling to be driven by a test-controlled scheduler.

## Decision

Introduce test mode through runtime components, not through workflow DSL flags.

For execute-step replacement, add a small internal runtime extension point:

```java
public interface ExecuteStepActionResolver {
    PayloadProcessor resolve(
            WorkflowContext workflowContext,
            WorkflowExecution workflowExecution,
            ExecutePrimitive.ExecuteCommand command
    );
}
```

The default runtime component returns `command.action()`.

`ExecuteDelegate` must call the resolver immediately before invoking the action and use the returned
`PayloadProcessor`. The delegate remains responsible for:

- publishing `STARTED`, `COMPLETED`, `FAILED`, `TIMED_OUT`, and `CANCELLED` events;
- computing the input payload with the command's parameter reducer;
- applying the command's result reducer through normal workflow events;
- preserving retry semantics through `RetryableExecuteDelegate`.

The test module registers a manual resolver component. That resolver stores per-step test actions and can block an
execute step after its `STARTED` event until the fixture supplies behavior:

- `executeStepReturning(stepName, payload)` supplies a fake processor returning `payload`;
- `executeStep(stepName, processor)` supplies the given fake processor;
- `executeStep(stepName)` releases the original production action;
- `executeStepFailing(stepName, failure)` supplies a fake processor throwing `failure`.

This lets `waitingIn(stepName)` observe the step in `STARTED` state without running real code.

The manual resolver is deliberately not a fallback resolver. If a workflow reaches an execute step for which the
fixture has not supplied behavior, the resolver creates a pending entry for that step and waits for it to be completed.
Absence of fixture instruction means "hold this step", not "run the production action". This prevents accidental side
effects during fixture tests and makes each transition explicit.

The step can be released in two ways:

- the fixture can supply a fake `PayloadProcessor`, which receives the real reduced input payload and
  `ProcessingContext`;
- the fixture can explicitly release the original workflow action for that step.

The runtime still owns step lifecycle semantics. `ExecuteDelegate` records the step as `STARTED` before resolving the
action. Only after the started state is visible does it ask the resolver for the action to invoke. This ordering is what
allows fixture assertions such as `waitingIn("activateUser")` to observe a running step while the
workflow driver thread is blocked before user code is invoked.

For deterministic time, add a second internal component:

```java
public interface WorkflowScheduler {
    ScheduledTask schedule(Instant deadline, Runnable task);
}
```

The default scheduler uses the configured `Clock` and wall-clock delayed execution. The test module registers a mutable
clock plus a manual scheduler. `timePasses(duration)` advances the mutable clock and executes all scheduler tasks whose
deadline is now due.

Timeout scheduling in `ExecuteDelegate`, `WaitForDelegate`, and retry backoff should use this scheduler instead of
direct `CompletableFuture.delayedExecutor(...)` or `Future.orTimeout(...)`.

## Scenarios

```gherkin
Scenario: execute step is held by the fixture
Given a workflow starts
When it reaches execute step "activateUser"
Then the fixture observes step "activateUser" as STARTED
And the real action has not been invoked

Scenario: test supplies successful execute result
Given execute step "activateUser" is STARTED
When the fixture supplies a payload processor returning {}
Then the runtime publishes the normal completed-step event
And the workflow continues with normal payload reducer behavior

Scenario: test releases the real action
Given execute step "activateUser" is STARTED
When the fixture calls executeStep("activateUser")
Then the original action from the workflow definition is invoked

Scenario: explicit time passage triggers timeout
Given wait step "waitForMagic" has timeout 5 seconds
When the fixture advances time by 5 seconds
Then the scheduler fires the timeout task
And the runtime publishes the normal timed-out step event
```

## Consequences

- Production workflows remain unchanged because the default resolver returns the original action.
- Test mode is activated by registering test components, not by changing workflow definitions or DSL APIs.
- The fake action still receives the real reduced payload and `ProcessingContext`.
- Completion, failure, cancellation, retry, timeout, and payload-reducer behavior stay in runtime code and are not
  duplicated in the fixture.
- Deterministic timeout testing requires replacing delayed future scheduling; a custom `Clock` alone cannot control
  existing wall-clock delayed futures.
- The execute-step resolver is the smallest useful runtime hook; the timeout scheduler is a separate hook because time
  control affects execute steps, wait steps, and retry backoff.
