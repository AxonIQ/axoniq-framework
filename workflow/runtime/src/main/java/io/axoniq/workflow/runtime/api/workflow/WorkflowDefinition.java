package io.axoniq.workflow.runtime.api.workflow;

import jakarta.annotation.Nonnull;

@FunctionalInterface
public interface WorkflowDefinition<T extends WorkflowContext> {

  void execute(@Nonnull T context);
}
