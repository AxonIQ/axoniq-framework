package io.axoniq.workflow.runtime.api.workflow;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

import java.util.Map;

@FunctionalInterface
public interface WorkflowContextFactory<T extends WorkflowContext> {
  @Nonnull
  T createContext(@Nonnull Map<String, Object> initialPayload,
                  @Nonnull StateManager stateManager,
                  @Nonnull EventAppender eventAppender
  );
}
