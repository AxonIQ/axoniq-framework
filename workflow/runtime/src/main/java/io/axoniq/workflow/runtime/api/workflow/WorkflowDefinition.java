package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.engine.StateManager;
import jakarta.annotation.Nonnull;

import java.util.Map;

public interface WorkflowDefinition<T extends WorkflowContext> {

  void execute(@Nonnull T context);

  T createContext(@Nonnull StateManager stateManager, @Nonnull Map<String, Object> trigger);

  String workflowId(@Nonnull Map<String, Object> trigger);
}
