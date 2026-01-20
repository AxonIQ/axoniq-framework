package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;

import java.util.Map;

public interface SimpleDefinition extends WorkflowDefinition<SimpleContext> {
  /**
   * Used for subclassing of own definitions.
   */
  abstract class Type implements SimpleDefinition {
    @Override
    public SimpleContext createContext(StateManager stateManager, Map<String, Object> trigger) {
      return new SimpleContext(workflowId(trigger), stateManager, trigger);
    }
  }
}
