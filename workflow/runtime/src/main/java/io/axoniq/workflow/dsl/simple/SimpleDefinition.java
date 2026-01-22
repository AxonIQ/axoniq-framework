package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import jakarta.annotation.Nonnull;

import java.util.Map;

public interface SimpleDefinition extends WorkflowDefinition<SimpleContext> {

  @Override
  default SimpleContext createContext(@Nonnull StateManager stateManager, @Nonnull Map<String, Object> trigger) {
    return new SimpleContext(workflowId(trigger), stateManager, trigger);
  }

  /**
   * Used for subclassing of own definitions.
   */
  abstract class Type implements SimpleDefinition {
  }
}
