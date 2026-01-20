package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.WorkflowContext;

public abstract class AbstractPrimitiveDelegate {

  protected final WorkflowContext context;

  public AbstractPrimitiveDelegate(WorkflowContext context) {
    this.context = context;
  }

}
