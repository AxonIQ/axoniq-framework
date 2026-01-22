package io.axoniq.workflow.runtime.api.workflow;

import jakarta.annotation.Nonnull;

public interface WorkflowStateFactory {
  WorkflowState create(@Nonnull WorkflowContext context);
}
