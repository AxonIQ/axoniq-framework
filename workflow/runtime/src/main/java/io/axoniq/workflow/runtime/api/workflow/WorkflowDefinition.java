package io.axoniq.workflow.runtime.api.workflow;

import jakarta.annotation.Nonnull;

/**
 * Definition of the workflow.
 * @param <T>
 */
@FunctionalInterface
public interface WorkflowDefinition<T extends WorkflowContext> {

  void execute(@Nonnull T context);
}
