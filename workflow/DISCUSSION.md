# Questions

## Question 1: How many events do we have?

Affected areas:
- Do we have explicit workflow lifecycle events and explicit step lifecycle events? Are they always there?  
- Example 1 for workflow lifecycle: WorkflowStartedEvent (technical) vs. OrderSubmitted (domain)
- Example 2 for step lifecycle for waiting for event: StepCompletedEvent (technical) vs. EmailReceived (domain)

Options:
- If only domain events are taken, we need a way to mark those domain events as relevant for the current workflow, after 
they are stored and correlated with workflow (in order to rehydrate the workflow state afterwards). => This will require DCB and not pollute the store with technical events. (✓) 
- If we have explicit events for all lifecycle transitions, we only need to source in events the workflow created => This will not require DCB at costs of technical events.
- We mix both approaches and generate event names out of the step definitions and pass their content into it. By doing so we provide unique event experience.
- Offer a QOS param on execute function, if QOS is configured, just skip publish of StepStartedEvent and proceed as usual. Only StepCompleted will be published, but if crash occurs function will be called twice.

## Question 2: How do we implement waiting for event?

Options:
- busy loop for non DCB and non Axon Server (✓)
- scheduled events with Axon Server and DCB - distributed durability
- postgress scheduler for postgres extension


## Question 3: Data flow in the workflow

Options:
- Use host language data flow only. For example in Java the variable definition scope is bound to a scope (class, method, block) and leaks to a sub-scope (non pure functional). 
  The problem might arise if the variables are filled with values retrieved by side effects, since those are not re-playable. 
  This kind of bugs is hard to chase (works in first attempt, doesn't work in re-hydration)
- Provide mechanisms for data access and scopes (workflow, step) and primitives for combination of those. (✓) 
  Default will be take step parameters from local context, merge result into global context (but return the local result).

  
## Question 4: What is the result of the execution?

Options: 
- A type - this is very close to pure functional style, but creates problems with "null" and needs explicit type system - lacks global / local idea and needs explicit data flow. 
- A Map<String, Object> with named variables. This approach is close to "common sense" in workflow systems. Disadvantage for one value only - but can work with defaults like "result" as the only one value.
- Both (internally use a map, but allow usage of types on DSL value) (✓)

## Question 5: What is time?

Options:
- Use natual time in timeout. => Leads to explicit modelling of wait steps (currently seems easier). (✓)
- Use Instance time => Allow to calculate everything in time from workflow start / previous step finished => No additional timeout lifecycle events needed, but more complex time calculations.


# TODO's
- Provide a way to modify event naming on a step
- Retries in execute? -> No it is part of the wrapper/decorator around the primitive
- Composition of workflows
    - Nested workflows
    - When parent workflow times out all child workflow timeout
