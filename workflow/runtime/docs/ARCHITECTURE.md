# Workflow Runtime Architecture Documentation

## Overview Diagram

```
                           ┌─────────────────────────────────────────────────────────────┐
                           │                      WORKFLOW RUNTIME                        │
                           └─────────────────────────────────────────────────────────────┘
                                                        │
         ┌──────────────────────────────────────────────┼──────────────────────────────────────────────┐
         │                                              │                                              │
         ▼                                              ▼                                              ▼
┌─────────────────┐                          ┌─────────────────┐                          ┌─────────────────┐
│   COORDINATOR   │─────────────────────────▶│  WORKFLOW ENGINE │◀────────────────────────│  STATE MANAGER  │
│                 │                          │                 │                          │                 │
│ • Event sub     │   restoreAndExecute()    │ • Initialize    │   getHistory()           │ • Event store   │
│ • Lifecycle     │──────────────────────────│ • Restore       │◀─────────────────────────│ • Subscribe     │
│ • Dedup         │                          │ • Execute       │                          │ • Append        │
└─────────────────┘                          └────────┬────────┘                          └─────────────────┘
                                                      │
                                                      │ Virtual Thread
                                                      ▼
                                             ┌─────────────────┐
                                             │ WORKFLOW CONTEXT│
                                             │                 │
                                             │ ┌─────────────┐ │
                                             │ │  Primitives │ │
                                             │ │ • execute() │ │
                                             │ │ • waitFor() │ │
                                             │ └─────────────┘ │
                                             └─────────────────┘
```

### Component Responsibilities

| Component | Responsibilities                                                                                                                                                             |
|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Coordinator** | Lifecycle management (start/stop), subscribes to trigger events (that start workflow), deduplication of workflow executions, delegates to WorkflowEngine                     |
| **Workflow Engine** | Orchestrates workflow lifecycle: initialize → restore → execute. Manages virtual thread execution. Coordinates between state and user code                                   |
| **State Manager** | Persists and retrieves workflow events. Provides `getHistory()` for restore, `append()` for new events, `subscribe()` for event notifications  (this component needs rework) |
| **Workflow Context** | Holds current workflow state (`WorkflowExecution`). Provides primitives (`execute()`, `waitFor()`) that user code interacts with                                             |

## Design Principles

### Separating State from Actions

The runtime follows an event sourcing pattern where state reconstruction is separated from action execution:

1. **Restore phase** (pure state reconstruction): Load historical events and rebuild `WorkflowExecution` state. This phase has no side effects - no I/O, no service calls, no event publishing. It is purely deterministic state reconstruction from events.

2. **Execute phase** (side effects allowed): Once state is restored, the workflow continues execution. User code runs here and may perform any side effects (API calls, database operations, etc.).

This mirrors the split between sourcing an entity's state from events and handling commands/actions.

### Side-Effect Safety in Restore

The `restore()` method is an internal engine operation that users cannot access or influence. User workflow code only interacts with primitives like `execute()` and `waitFor()` during the execute phase. This architectural boundary guarantees that state reconstruction remains pure and deterministic.

### Restore-Then-Execute Flow

When a workflow is triggered (or recovered after a crash):

1. Load all historical events for this workflow ID from the StateManager
2. Replay events to rebuild step execution state (finite operation)
3. Once history is fully replayed, begin/continue execution

There is no "catching up" to a moving target - history replay is a bounded operation that completes before execution resumes.

### In-Flight State Updates

When `execute(step)` runs, it performs two coupled operations:

1. **Publishes event** to the StateManager (for future rehydration)
2. **Updates local state immediately** (for current execution)

```java
// From context/AbstractPrimitiveDelegate.java
protected void started(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    state.eventAppender().append(startedStep(context, stepName, payload, eventNameCustomizer)); // publish event
    state.addStep(StepExecution.started(stepName, Instant.now(state.getClock()))); // update local state
}
```

This coupling allows workflow execution to drive forward without requiring rehydration from history after each step. The workflow maintains its own in-memory state while simultaneously persisting events for durability.

## Core Flow (Sequence Diagram)

```
Trigger Event → Coordinator → WorkflowEngine → WorkflowContext → Steps
     │              │               │               │              │
     │   subscribe  │               │               │              │
     ├─────────────▶│               │               │              │
     │              │ restoreAndExecute()           │              │
     │              ├──────────────▶│               │              │
     │              │               │ initialize()  │              │
     │              │               ├──────────────▶│              │
     │              │               │ restore()     │              │
     │              │               │◀──────────────┤ (replay events)
     │              │               │ execute()     │              │
     │              │               ├──────────────▶│              │
     │              │               │               │ execute(step)│
     │              │               │               ├─────────────▶│
     │              │               │               │◀─────────────┤
     │              │               │◀──────────────┤              │
     │              │◀──────────────┤               │              │
     │              │  (completed)  │               │              │
```

## State Data Structures

```
WorkflowExecution
├── workflowId: String
├── status: WorkflowStatus (STARTED | COMPLETED | FAILED | CANCELLED | TIMED_OUT)
├── payload: Map<String, Object>
└── steps: Map<String, StepExecution>
                          │
                          └─▶ StepExecution (record)
                              ├── stepName: String
                              ├── status: StepStatus (STARTED | COMPLETED | FAILED | TIMED_OUT)
                              ├── result: Object
                              └── error: Throwable
```

## Primitives Table

| Primitive | Purpose | Recovery Behavior |
|-----------|---------|-------------------|
| `execute()` | Run sync action | Returns cached result if COMPLETED |
| `waitFor()` | Wait for external event | Recalculates remaining timeout from STARTED |
| `wait()` | Pause execution | Same as waitFor (implemented via waitFor) |

## DSL Architecture

The DSL is organized in layers, separating low-level primitives from user-friendly APIs. Both **Java** and **Kotlin** DSLs are available, built on the same underlying primitives.

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Workflow Code                        │
│                     implements SimpleDefinition                  │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SimpleContext                           │
│            (extends WorkflowExecution, user-facing)             │
│  ┌─────────────────────┐    ┌─────────────────────┐             │
│  │ExecuteInLocalContext│    │    WaitForEvent     │             │
│  │ • execute(name, λ)  │    │ • waitForEvent()    │             │
│  │ • execute(Payload)  │    │ • wait(duration)    │             │
│  └─────────────────────┘    └─────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                       API Primitives                             │
│                    (runtime/api/primitives/)                     │
│  ┌─────────────────────┐    ┌─────────────────────┐             │
│  │   ExecutePrimitive  │    │   WaitForPrimitive  │             │
│  │ Full signature with │    │ Full signature with │             │
│  │ timeout, reducers,  │    │ predicate, timeout, │             │
│  │ event customizer    │    │ converter           │             │
│  └─────────────────────┘    └─────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer | Package | Purpose |
|-------|---------|---------|
| **API Primitives** | `runtime/api/primitives/` | Low-level interfaces with full control (timeouts, payload reducers, event customizers) |
| **DSL Interfaces** | `dsl/simple/` | User-friendly wrappers with sensible defaults. `ExecuteInLocalContext` and `WaitForEvent` provide overloaded methods |
| **SimpleContext** | `dsl/simple/` | Combines all DSL interfaces into one context object passed to user workflows |
| **SimpleDefinition** | `dsl/simple/` | Bundles workflow definition with context factory, state factory, and association provider |

### SimpleDefinition Consolidation

`SimpleDefinition` implements multiple interfaces to reduce boilerplate:

```java
public interface SimpleDefinition extends
    WorkflowDefinition<SimpleContext>,     // execute(ctx) method
    WorkflowContextFactory<SimpleContext>, // creates SimpleContext
    WorkflowStateFactory,                  // creates WorkflowState
    AssociationProvider,                   // extracts workflow ID from trigger
    WorkflowConfiguration<SimpleContext>   // bundles all above
```

Users only need to implement two methods:
- `association(payload)` → returns workflow ID
- `execute(ctx)` → workflow logic using primitives

### DSL File Reference

| File | Purpose                                                       |
|------|---------------------------------------------------------------|
| `dsl/simple/SimpleDefinition.java` | All-in-one interface for workflow definitions                 |
| `dsl/simple/SimpleContext.java` | Context passed to user code, extends `WorkflowExecution`      |
| `dsl/simple/ExecuteInLocalContext.java` | Convenience methods for `execute()` primitive                 |
| `dsl/simple/WaitForEvent.java` | Convenience methods for `waitFor()` primitive                 |
| `dsl/simple/Payload.java` | Fluent wrapper for `Map<String, Object>` payload manipulation |


## Event Metadata

```
MetadataUtils Keys:
├── METADATA_KEY_WORKFLOW_ID  → workflow identifier
├── METADATA_KEY_STEP_NAME    → step name
├── METADATA_KEY_TYPE         → step status (STARTED/COMPLETED/FAILED/TIMED_OUT)
└── METADATA_KEY_WORKFLOW_STATUS → workflow status
```

## Event Name Customization

The runtime publishes events for workflow and step lifecycle transitions. By default, event names follow a convention, but can be fully customized via `EventNameCustomizer`.

### Default Event Name Format

```
{namespace}.{BaseName}{Status}{Version}
```

**Default values:**
- `namespace`: `io.axoniq.workflow`
- `baseName`: step name (capitalized)
- `status`: `Started`, `Completed`, `Failed`, `TimedOut`
- `version`: `#0.1`

**Example:** Step named `createUser` completing → `io.axoniq.workflow.CreateUserCompleted#0.1`

### Customization Options

| Option | Default | Description |
|--------|---------|-------------|
| `namespace(String)` | `io.axoniq.workflow` | Event namespace prefix |
| `baseName(String)` | step name | Override the base name |
| `baseVersion(String)` | `#0.1` | Version suffix |
| `appendToBaseName(boolean)` | `true` | Include namespace and base name |
| `appendVersion(boolean)` | `true` | Include version suffix |
| `capitalizeSimpleName(boolean)` | `true` | Capitalize base name |
| `stepStarted(String)` | `Started` | Status suffix for STARTED |
| `stepCompleted(String)` | `Completed` | Status suffix for COMPLETED |
| `stepFailed(String)` | `Failed` | Status suffix for FAILED |
| `stepTimedOut(String)` | `TimedOut` | Status suffix for TIMED_OUT |
| `payloadCustomization(Function)` | identity | Dynamic name based on payload |

### Usage Example

```java
// Using defaults
ctx.execute("sendEmail", () -> { /* ... */ });
// Event name: io.axoniq.workflow.SendEmailCompleted#0.1

// Custom namespace and base name
ctx.execute("getValue", String.class, () -> "result",
    namespace("com.myapp.signup")
        .baseName("UserCreation")
        .stepStarted("Initiated")
);
// Started event: com.myapp.signup.UserCreationInitiated#0.1
// Completed event: com.myapp.signup.UserCreationCompleted#0.1
```

### Event Payload Content

Events carry payload data that varies by event type:

| Event Type | Payload Content |
|------------|-----------------|
| **STARTED** | Input arguments passed to the step (local payload) |
| **COMPLETED** | Return value from the step action |
| **FAILED** | Exception information |
| **TIMED_OUT** | Timeout timestamp |

### Controlling Event Payloads

The `execute()` primitive accepts a local payload that becomes the STARTED event payload. The return value from your action becomes the COMPLETED event payload.

```java
// Simple execute - no input payload, no return value
ctx.execute("notify", () -> sendNotification());
// STARTED payload: {}
// COMPLETED payload: {}

// Execute with return value
String userId = ctx.execute("createUser", String.class, () -> {
    return userService.create(email);
});
// STARTED payload: {}
// COMPLETED payload: {"__createUser": "user-123"}

// Execute with input payload
ctx.execute("processOrder",
    payload("orderId", "order-456", "amount", 100),
    (input) -> {
        orderService.process(input.get("orderId"), input.get("amount"));
        return payload("status", "processed");
    }
);
// STARTED payload: {"orderId": "order-456", "amount": 100}
// COMPLETED payload: {"status": "processed"}
```

### Payload Reducers (Advanced)

For fine-grained control over what data flows into step execution and back to the workflow context, use `PayloadReducer`:

| Reducer | Behavior |
|---------|----------|
| `local()` | Use only the local payload passed to execute |
| `global()` | Use only the workflow-level payload |
| `all()` | Merge local and global payloads |
| `none()` | Empty payload |

The DSL defaults to `local()` for parameters and `all()` for results, meaning:
- Steps receive only their explicit input (isolation)
- Results are merged back into the workflow payload (accumulation)

### Implementation

| File | Purpose |
|------|---------|
| `api/primitives/EventNameCustomizer.java` | Interface for event name generation |
| `context/DefaultEventNameCustomizer.java` | Fluent builder with defaults |
| `api/primitives/PayloadReducer.java` | Controls payload flow between workflow and steps |


## File Reference Table

| Component | File | Key Methods |
|-----------|------|-------------|
| Coordinator | `engine/Coordinator.java` | `start()`, `stop()`, `subscribeForTriggerEvents()` |
| Engine | `engine/WorkflowEngine.java` | `restoreAndExecute()`, `initialize()`, `restore()`, `execute()` |
| State Manager | `engine/SimpleStateManager.java` | `getHistory()`, `subscribe()`, `append()` |
| Context | `context/WorkflowExecution.java` | Holds workflow state, delegates to primitives |
| Execute | `context/ExecuteDelegate.java` | Runs steps with caching, virtual threads |
| WaitFor | `context/WaitForDelegate.java` | Event subscription with timeout recovery |
| Step State | `engine/StepExecution.java` | Immutable record for step tracking |
| DSL | `dsl/simple/SimpleContext.java` | Convenience wrapper for user workflows |

All file paths relative to `runtime/src/main/java/io/axoniq/workflow/runtime/`

## Error Handling

### Terminal vs Non-Terminal Failures

The runtime distinguishes between two types of errors:

| Error Type | Exception | Workflow Status | Behavior |
|------------|-----------|-----------------|----------|
| **Terminal** | `WorkflowFailedException` | `FAILED` | Workflow ends permanently, failure event published |
| **Non-Terminal** | Any other exception | Stays `STARTED` | Workflow can recover on restart (TODO: retry mechanism) |

### When to Use WorkflowFailedException

Throw `WorkflowFailedException` when the workflow should **permanently fail** and not be retried:

```java
@Override
public void execute(SimpleContext ctx) {
    var result = ctx.execute("validateInput", () -> validate(input));

    if (!result.isValid()) {
        // Terminal failure - workflow cannot proceed
        throw new WorkflowFailedException("Invalid input: " + result.getError());
    }

    // Continue with workflow...
}
```

### Handling Step Errors with Try-Catch

Use standard try-catch to handle errors gracefully within the workflow. This allows the workflow to continue or take alternative paths:

```java
@Override
public void execute(SimpleContext ctx) {
    ctx.execute("createUser", () -> createUser());

    try {
        // Wait for external confirmation with timeout
        var confirmed = ctx.waitForEvent("emailConfirmed", EmailConfirmed.class, Duration.ofSeconds(30));

        ctx.execute("sendWelcome", () -> sendWelcomeEmail(confirmed.email()));
    } catch (Exception e) {
        // Timeout or other error - workflow continues without welcome email
        logger.warn("Email confirmation failed: {}", e.getMessage());
    }

    // Workflow completes successfully even if email confirmation failed
    ctx.execute("finalizeAccount", () -> finalize());
}
```

### Step-Level Errors

When a step throws an exception:

1. Step status becomes `FAILED`
2. `StepFailedException` is thrown (wraps original exception)
3. Can be caught and handled in workflow code
4. If uncaught, propagates up (non-terminal by default)

```java
try {
    ctx.execute("riskyOperation", () -> {
        throw new RuntimeException("Something went wrong");
    });
} catch (StepFailedException e) {
    // Handle step failure, maybe retry or take alternative action
    ctx.execute("fallbackOperation", () -> doFallback());
}
```

### Error Handling Summary

```
Exception in step action
         │
         ▼
    Step marked FAILED
         │
         ▼
  StepFailedException thrown
         │
         ├─▶ Caught in workflow code → Handle gracefully, continue
         │
         └─▶ Uncaught, propagates up
                    │
                    ├─▶ WorkflowFailedException → Workflow FAILED (terminal)
                    │
                    └─▶ Other RuntimeException → Workflow stays STARTED (recoverable on restart)
```
## Recovery Flow

```
Crash → New Coordinator → WorkflowEngine.restore()
                              │
                              ├─▶ Load events from StateManager (finite history)
                              │
                              ├─▶ Rebuild StepExecution map (pure, no side effects)
                              │     • COMPLETED → return cached result
                              │     • FAILED → return cached error
                              │     • STARTED → recalculate timeout, retry
                              │
                              └─▶ Continue execution from last state (side effects resume)
```

The restore phase is deterministic: given the same event history, the same state is always reconstructed. User code is not invoked during restore - only during the subsequent execute phase.

## Quick Start Example

```java
// 1. Define workflow
public class MyWorkflow implements SimpleDefinition {
    @Override
    public String association(Map<String, Object> trigger) {
        return "workflow-" + trigger.get("id");
    }

    @Override
    public void execute(SimpleContext ctx) {
        ctx.execute("step1", () -> { /* logic */ });
        ctx.waitForEvent("approval", ApprovalEvent.class);
        ctx.execute("step2", () -> { /* logic */ });
    }
}

// 2. Register and start
var stateManager = new SimpleStateManager();
var coordinator = new Coordinator(stateManager, stateManager);
coordinator.declarative().register(
    new QualifiedName(TriggerEvent.class),
    new MyWorkflow()
);
coordinator.start();
```

## Test Suite

All tests located in `runtime/src/test/`

### Java Tests

| Test | Description |
|------|-------------|
| [`CoordinatorUserSignupTest`](../src/test/java/io/axoniq/workflow/runtime/CoordinatorUserSignupTest.java) | End-to-end test with Coordinator. User signup flow with trigger events, `waitForEvent`, and step execution. Good starting point for understanding the full stack. |
| [`UserSignupTest`](../src/test/java/io/axoniq/workflow/runtime/UserSignupTest.java) | Basic workflow execution using WorkflowEngine directly. Tests step caching, event name customization, and replay behavior. |
| [`UserSignupPayloadTest`](../src/test/java/io/axoniq/workflow/runtime/UserSignupPayloadTest.java) | Payload flow between steps. Tests input/output payload handling and `Payload` helper class. |
| [`WorkflowFailedExceptionTest`](../src/test/java/io/axoniq/workflow/runtime/WorkflowFailedExceptionTest.java) | Error handling. Verifies terminal (`WorkflowFailedException`) vs non-terminal exceptions behavior. |
| [`LoanApplicationTimeoutTest`](../src/test/java/io/axoniq/workflow/runtime/LoanApplicationTimeoutTest.java) | Timeout handling and compensation. Tests step timeouts with slow external services and compensation logic. |
| [`ParallelWorkflowSubscriptionTest`](../src/test/java/io/axoniq/workflow/runtime/ParallelWorkflowSubscriptionTest.java) | Parallel workflow execution. Tests multiple concurrent workflows, event routing to correct instances, and subscription-based notifications. |
| [`OrderFulfillmentWorkflowTest`](../src/test/java/io/axoniq/workflow/runtime/OrderFulfillmentWorkflowTest.java) | Real-world scenario. Order processing with inventory check, payment, and shipping steps. |
| [`TravelBookingParallelTest`](../src/test/java/io/axoniq/workflow/runtime/TravelBookingParallelTest.java) | Parallel step execution within a workflow. Books flight, hotel, and car concurrently using `CompletableFuture`. |
| [`WorkflowCrashRecoveryTest`](../src/test/java/io/axoniq/workflow/runtime/WorkflowCrashRecoveryTest.java) | Crash recovery via event sourcing. Tests that completed steps aren't re-executed after restart. *(Currently disabled - requires Coordinator changes)* |
| [`JacksonSerializationTest`](../src/test/java/io/axoniq/workflow/runtime/JacksonSerializationTest.java) | Serialization. Verifies Jackson can serialize POJOs, records, and maps for event payloads. |

### Kotlin Tests

| Test | Description |
|------|-------------|
| [`UserSignupPayloadTest.kt`](../src/test/kotlin/UserSignupPayloadTest.kt) | Kotlin DSL example. Same user signup flow using Kotlin DSL with `KDefinition` and `Kontext`. Demonstrates try-catch error handling. |

---

## Roadmap

### PoC / 0.1.0

- [x] **Coordinator** - Lifecycle management, trigger event subscription, deduplication
- [x] **execute() primitive** - Run synchronous actions with caching and timeout
- [x] **waitForEvent() primitive** - Subscribe and wait for external events with timeout
- [x] **Java DSL** - `SimpleDefinition`, `SimpleContext`, `Payload` helper

### MVP / 1.0.0

- [x] **Error handling** - `WorkflowFailedException` for terminal failures, try-catch for graceful handling
- [ ] **Composables** - `parallelRun()` syntactic sugar for concurrent step execution - can be achieved already via host language
- [x] **Describable / Projection** - Share workflow state for external consumption  - partially implemented via `describeTo()`
- [x] **DSL Engine** - Pluggable DSL execution layer
- [x] **Kotlin DSL** - `KDefinition`, `Kontext` with idiomatic Kotlin syntax
- [ ] **Axon 5 Integration** - move to dedicated module and connect to real Axon components
- [ ] **Testing support** - Test fixtures, time manipulation, event injection utilities

---

## Open Questions & Discussion

Open design questions, architectural decisions, and topics under discussion:

[DISCUSSION.md](../../DISCUSSION.md)
    