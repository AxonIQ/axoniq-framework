package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.StateManager;

import java.util.Map;

public interface WorkflowDefinition<T extends WorkflowContext> {

  void execute(T context);

  T createContext(String workflowId, StateManager stateManager, Map<String, Object> trigger);
}
