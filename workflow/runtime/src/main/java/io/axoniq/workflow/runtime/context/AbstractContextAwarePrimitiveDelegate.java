package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import jakarta.annotation.Nonnull;

public abstract class AbstractContextAwarePrimitiveDelegate {

  protected final WorkflowContext context;
  protected final WorkflowState lifecycle;

  public AbstractContextAwarePrimitiveDelegate(@Nonnull WorkflowContext context, @Nonnull WorkflowState lifecycle) {
    this.context = context;
    this.lifecycle = lifecycle;
  }
}
