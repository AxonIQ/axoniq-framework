# Questions

---

## Question 1: What was wrong in AF4 Saga?

- Single steps were easy to describe, but the workflow was not visible.
- Own persistence based on internal state of the Saga class

---

## Question 2: What should the workflow definition look like to become more visible/simple/transparent?

### Option 1: Imperative model using the host language (Java).

#### Pro:

- Only few primitives are required, the remaining part is the host language

#### Contra:

- Host language is hard to control (cancellation, suspension) between the steps

### Option 2: Declarative model constructing a workflow as a graph of nodes that is traversed

#### Pro:

- Compatible with other declarative models (e.g. BPMN)
- Easier to build for cancellation/suspension between steps

#### Contra:

- Will need the whole language (not only primitives)

---

## Question 3: What is the execution model of the workflow?

### Option 1: State-based

The workflow definition is a graph of states and the engine holds the pointer to the current step.

#### Pro:

- The workflow always is in one state. Every state can be seen as a snapshot.
- Works nicely with declarative definition approach.

#### Contra:

- Transitions between states might require external trigger / heart-beat.

### Option 2: Stack-based

The workflow definition is a set of instructions. On execute it either blocks the execution or terminates with a special
suspension on every wait state in order to be restarted and continue execution.

#### Pro:

- Works nicely with imperative approach.
- If blocking style is selected, the workflow is executed as long as the definition has instructions.

#### Contra:

- Suspension / Migration needs to be carefully designed.
- If blocking is not allowed, the waiting states must throw a `ExecutionSuspended` to reach the target state and then
  needs to be wake up. 

---

## Question 4: How many events do we have?

Affected areas:

- Do we have explicit workflow lifecycle events and explicit step lifecycle events? Are they always there?
- Example 1 for workflow lifecycle: WorkflowStartedEvent (technical) vs. OrderSubmitted (domain)
- Example 2 for step lifecycle for waiting for event: StepCompletedEvent (technical) vs. EmailReceived (domain)

Example:

```
── workflowEvents/
   └── [0]/
       ├── _ref: 553879264
       ├── _type: io.axoniq.workflow.runtime.engine.execution.PrettyPrintingRecordingEventStore$WorkflowEventDescriptor
       └── signup-user-456/
           ├── [0]: io.axoniq.workflow.Signup-user-456Started (none): {}
           ├── [1]: io.axoniq.workflow.CreateUserStarted (STARTED): {}
           ├── [2]: io.axoniq.workflow.CreateUserCompleted (COMPLETED): {__createUser=true}
           ├── [3]: io.axoniq.workflow.ActivateUserStarted (STARTED): {}
           ├── [4]: io.axoniq.workflow.ActivateUserCompleted (COMPLETED): {}
           ├── [5]: io.axoniq.workflow.SendWelcomeEmailStarted (STARTED): {}
           ├── [6]: io.axoniq.workflow.SendWelcomeEmailCompleted (COMPLETED): {}
           ├── [7]: io.axoniq.workflow.WaitASecondStarted (STARTED): {duration=PT1S, started=2026-01-28T17:48:03.604382Z}
           ├── [8]: io.axoniq.workflow.WaitASecondCompleted (TIMED_OUT): 2026-01-28T17:48:04.605768Z
           ├── [9]: io.axoniq.workflow.WaitForMagicToHappenStarted (STARTED): {duration=PT5S, started=2026-01-28T17:48:04.607512Z}
           ├── [10]: io.axoniq.workflow.WaitForMagicToHappenCompleted (COMPLETED): {magician=Merlin}
           └── [11]: io.axoniq.workflow.Signup-user-456Completed (none): {}
```

Options:

- If only domain events are taken, we need a way to mark those domain events as relevant for the current workflow, after
  they are stored and correlated with workflow (in order to rehydrate the workflow state afterward). => Will this
  require DCB and not pollute the store with technical events?

- If we have explicit events for all lifecycle transitions, we only need to source in events the workflow created =>
  Will this not require DCB at costs of technical events?

- If we mix both approaches and generate event names out of the step definitions and pass their content into it. By
  doing so we provide unique event experience.

- Offer a QOS param on execute function, if QOS is configured, just skip publish of StepStartedEvent and proceed as
  usual. Only StepCompleted will be published, but if crash occurs function will be called twice.

---

## Question 5: How do we implement waiting for event?

Notes:
- There always should be a timeout if we wait for event.
- Either we get the event in time or the timeout must fire.

Options:

- busy loop for non DCB and non Axon Server?
- completable future waiting for trigger to be received?
- external timeout mechanism (scheduler, runr)?
- scheduled events with Axon Server and DCB - distributed durability?
- postgresql scheduler for postgres extension?

---

## Question 6: Data flow in the workflow

Options:

- Use host language data flow only. For example in Java the variable definition scope is bound to a scope (class,
  method, block) and leaks to a sub-scope (not pure functional).
  The problem might arise if the variables are filled with values retrieved by side effects, since those are not
  re-playable. This kind of bugs is hard to chase (works in first attempt, doesn't work in re-hydration). This should be 
  default because it doesn't need additional knowledge.

- Provide additional mechanisms for data access and scopes (workflow, step) and primitives for combination of those. This
  is more advanced for complex workflows and standard approach in workflow engines.

---

## Question 7: What is the result of a step execution?

Options:

- A type - this is very close to pure functional style, but creates problems with "null" and needs explicit type
  system - lacks global / local idea and needs explicit data flow.

- A Map<String, Object> with named variables. This approach is close to "common sense" in workflow systems. Disadvantage
  for one value only - but can work with defaults like "result" as the only one value.

- Both (internally use a map, but allow usage of types on DSL value)

## Question 8: What is time?

Options:

- Use natual time in timeout. => Leads to explicit modelling of wait steps (currently seems easier).
- Use workflow time => Allow to calculate everything in time from workflow start / previous step finished => No
  additional timeout lifecycle events needed, but more complex time calculations. Might be an add-on.

---

## Question 9: How do we want to configure retries?

Options:

- Special parameters to the primitives

- External "Decorator" around the primitive with ability to be invoked from DSL

---

## Question 10: Do we want to support nested workflows?

Ideas:

- No -> otherwise it got too complex
- Yes:
  - What is the use case?
  - Is it more than just emit an event from the parent and wait for completion event?
  - What cross-level operations should we support (cascading cancellation, etc)?

---

## Question 11: How does cancellation works?

Ideas:
- Cancellation only in a waiting step.
- Should there be a cancellation handler?

---

## Question 12: Does the workflow has a timeout-handler?

---

## Question 12: Does the workflow has a cancellation / cancellation handler?

Ideas:
- Who wants to cancel workflow? (Another workflow? )
- Is there a cancellation an event?

---


