package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;

public abstract class AbstractContextAwarePrimitiveDelegate {

  protected final WorkflowContext context;

  public AbstractContextAwarePrimitiveDelegate(WorkflowContext context) {
    this.context = context;
  }
}
