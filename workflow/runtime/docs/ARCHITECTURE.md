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
                              ├─▶ Load events from StateManager
                              │
                              ├─▶ Rebuild StepExecution map
                              │     • COMPLETED → return cached result
                              │     • FAILED → return cached error
                              │     • STARTED → recalculate timeout, retry
                              │
                              └─▶ Continue execution from last state
```

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
