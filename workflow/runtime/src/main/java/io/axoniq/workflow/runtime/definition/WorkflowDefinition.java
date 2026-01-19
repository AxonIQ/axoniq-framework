package io.axoniq.workflow.runtime.definition;

import io.axoniq.workflow.runtime.context.WorkflowContextImpl;

public interface WorkflowDefinition {
  void execute(WorkflowContextImpl context);
}
