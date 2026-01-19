package io.axoniq.workflow.runtime.definition;

import io.axoniq.workflow.runtime.context.WorkflowContext;

import java.util.function.Consumer;

public interface WorkflowDefinition extends Consumer<WorkflowContext> {

  void execute(WorkflowContext context);

  default void accept(WorkflowContext context) {
    execute(context);
  }
}
