# ADR-012: Fixture failures must be explicit and phase-aware

- Name: Fixture failures must be explicit and phase-aware
- Status: Accepted
- Date: 2026-06-26

## Context

The workflow test fixture is a BDD-style API with distinct `given`, `when`, and `then` phases.

Tests use eventual assertions because workflow progress may be asynchronous. Awaitility is useful for retrying until a
condition becomes true, but its default timeout failures are too implementation-oriented for fixture users:

- they expose polling aliases and internal timeout mechanics;
- they blur the difference between a test action failure and an unmet assertion;
- they often surface AssertJ detail that does not clearly explain what workflow fact was expected.

This is especially problematic when diagnosing fixture failures such as:

- a `then().waitingIn("sendWelcomeEmail")` expectation that is false because the workflow is actually waiting in
  `activateUser2`;
- an `executeStepReturning("sendWelcomeEmail", ...)` action that is invalid because `sendWelcomeEmail` has not started
  yet.

The fixture should make these failures immediately understandable from the top-level message alone.

## Decision

The fixture reports failures using explicit domain messages and phase-specific failure types.

### Phase-specific failure types

- `given` and `when` failures are fixture action failures and throw `WorkflowTestFixtureException`.
- `then` failures are unmet expectations and throw `AssertionError`.

This distinction gives clear visibility into whether the test failed while driving the workflow or while asserting on
workflow state.

### Awaitility stays internal

Awaitility is still used for retry semantics through `untilAsserted(...)`, but it is treated as an internal
implementation detail.

- Polling continues until success or timeout.
- The fixture extracts or builds a domain-specific message from the last failed assertion.
- The user-facing exception does not include `ConditionTimeoutException` as the surfaced cause.

This keeps eventual consistency support while hiding retry mechanics from fixture users.

### Assertion messages must describe workflow facts

Assertions in `WorkflowEngineTestingState` and `WorkflowTestDriver` must not rely on generic `describeAs(...)`
messages.

Instead, each failure message must directly state:

- what was expected;
- what actually happened;
- and, when useful, the relevant current workflow state.

Examples:

- `Expected current workflow execution to be waiting in step 'sendWelcomeEmail', but it was waiting in step 'activateUser2'.`
- `Could not execute step 'sendWelcomeEmail' using the supplied test action because that step was not started. Current workflow execution is waiting in step 'activateUser2'. Finished steps: [createUser, activateUser].`
- `Expected workflow state to contain exactly [createUser, activateUser, sendWelcomeEmail] in order, but actual steps are [createUser, activateUser, activateUser2].`

### Assertion naming convention

Low-level assertion helpers in `WorkflowTestDriver` and `WorkflowEngineTestingState` follow a strict naming rule:

- `void xxxSatisfies(Consumer<XXX> xxxConsumer)`
- `XXX xxxMatches(Predicate<XXX> xxxPredicate)`
- `XXX xxxExists()`, implemented as `return xxxMatches(x -> true);`

Meaning:

- `...Satisfies(...)` is consumer-style inspection and does not return a value.
- `...Matches(...)` is predicate-style selection/assertion and returns the matched object.
- `...Exists()` is the existence-specialized shortcut and returns the selected object.

This keeps the distinction between assertion styles visible in the API surface and aligns the driver and testing state
with the fixture phase vocabulary.

### Guard tests

Unit tests in the `test` module must guard these messages so future refactors do not regress them into generic
framework output.

## Consequences

- Fixture users can identify the failing workflow fact from the first exception line.
- Test failures clearly distinguish action problems from expectation problems.
- Awaitility remains useful for retry behavior without leaking complexity into fixture output.
- The helper API has a predictable naming and return-type contract across fixture phases, driver, and testing state.
- Assertion methods require custom message code instead of generic AssertJ descriptions.
- Message wording is now part of the supported behavior of the fixture and must be maintained by tests.
