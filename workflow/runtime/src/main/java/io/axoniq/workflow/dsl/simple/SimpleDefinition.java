package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;

import java.util.Map;

public interface SimpleDefinition extends WorkflowDefinition<SimpleContext> {
  abstract class Type implements SimpleDefinition {
    @Override
    public SimpleContext createContext(String workflowId, StateManager stateManager, Map<String, Object> trigger) {
      return new SimpleContext(workflowId, stateManager, trigger);
    }
  }
}
