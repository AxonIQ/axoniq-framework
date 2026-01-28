# Souring approach

## 

- Registration takes places (QualifiedName -> WorkflowConfiguration)
- Start Coordinator
  - Start Event Handling Component (MessageStream.onNext())
  - Token=null -> Stream from Global Identifier = 0
  - OnEvent (apply predicate from WorkflowConfiguration)
    - Found event starting workflow! => 
      - Coordinator => WorkflowEngine.initialize() ( = create anew context with workflowId )
      - Coordinator => WorkflowEngine.restore() ( = Source all events for workflowId, finite stream )
  - Coordinator => WorkflowEngine.execute() ( = continue to listen to event handling component ), MessageStream.onNext()

## Summary

**Stream events** until first event matching registered WorkflowConfiguration, then create a WorkflowContext with id, 
**source its WorkflowState** and continue streaming.

# Streaming approach

## 

- Registration takes places (QualifiedName -> WorkflowConfiguration)
- Start Coordinator
    - Start Event Handling Component (MessageStream.onNext())
    - Token=null -> Stream from Global Identifier = 0
    - OnEvent (apply predicate from WorkflowConfiguration)
        - Found event starting workflow! =>
            - Coordinator => WorkflowEngine.initialize() ( = create anew context with workflowId )
    - On Event (if Workflow Event)
      - Change State
    - Listen to caught-up (`TrackerStatus` Listener)
      - Coordinator => WorkflowEngine.execute() ( = continue to listen to event handling component ), MessageStream.onNext()

## Summary

**Stream events** until first event matching registered WorkflowConfiguration, then create a WorkflowContext with id.