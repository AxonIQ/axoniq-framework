package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.engine.StateManager;

import java.util.Map;

public interface WorkflowDefinition<T extends WorkflowContext> {

  void execute(T context);

  T createContext(StateManager stateManager, Map<String, Object> trigger);

  String workflowId(Map<String, Object> trigger);
}
