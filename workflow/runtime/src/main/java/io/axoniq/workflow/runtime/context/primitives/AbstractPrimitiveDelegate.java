package io.axoniq.workflow.runtime.context.primitives;

import io.axoniq.workflow.runtime.context.WorkflowContextImpl;

public abstract class AbstractPrimitiveDelegate {
  protected final WorkflowContextImpl context;

  public AbstractPrimitiveDelegate(WorkflowContextImpl context) {
    this.context = context;
  }
}
