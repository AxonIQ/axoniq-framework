package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;

public abstract class AbstractPrimitiveDelegate {

  protected final WorkflowContext context;

  public AbstractPrimitiveDelegate(WorkflowContext context) {
    this.context = context;
  }

}
