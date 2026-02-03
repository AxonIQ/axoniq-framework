package io.axoniq.workflow.runtime.api.workflow;

import jakarta.annotation.Nonnull;

import java.util.Map;

/**
 * Creates a context for workflow execution.
 * @param <T> type of the context.
 */
@FunctionalInterface
public interface WorkflowContextFactory<T extends WorkflowContext> {
  @Nonnull
  T createContext(@Nonnull Map<String, Object> initialPayload, @Nonnull WorkflowServices workFlowServices);
}
