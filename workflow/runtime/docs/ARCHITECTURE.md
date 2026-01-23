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
│ • Event sub     │   restoreAndExecute()    │ • Initialize    │   getHistory()          │ • Event store   │
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

## Event Metadata

```
MetadataUtils Keys:
├── METADATA_KEY_WORKFLOW_ID  → workflow identifier
├── METADATA_KEY_STEP_NAME    → step name
├── METADATA_KEY_TYPE         → step status (STARTED/COMPLETED/FAILED/TIMED_OUT)
└── METADATA_KEY_WORKFLOW_STATUS → workflow status
```

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

## Virtual Threads

- `WorkflowEngine` uses `Executors.newVirtualThreadPerTaskExecutor()` for workflow execution
- `ExecuteDelegate` uses separate virtual thread executor for step execution
- Enables thousands of concurrent workflows without thread pool limits

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

## Test Reference

See `runtime/src/test/java/io/axoniq/workflow/runtime/CoordinatorUserSignupTest.java` for a complete working example.
