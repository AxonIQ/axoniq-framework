package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.engine.StateManager;
import jakarta.annotation.Nonnull;

import java.util.Map;

@FunctionalInterface
public interface WorkflowContextFactory<T extends WorkflowContext> {
  @Nonnull
  T createContext(@Nonnull Map<String, Object> initialPayload, @Nonnull StateManager stateManager);
}
