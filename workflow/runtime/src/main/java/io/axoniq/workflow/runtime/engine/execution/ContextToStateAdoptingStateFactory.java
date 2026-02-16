package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

import static io.axoniq.workflow.runtime.engine.execution.WorkflowState.requireIsWorkflowState;

public class ContextToStateAdoptingStateFactory<C extends WorkflowContext> implements WorkflowStateFactory {

  private final Class<C> workflowContextType;

  public ContextToStateAdoptingStateFactory(@Nonnull Class<C> workflowContextType) {
    this.workflowContextType = requireIsWorkflowState(workflowContextType);
  }

  @Override
  @Nonnull
  public WorkflowState create(@NotNull WorkflowContext context) {
    if (workflowContextType.isAssignableFrom(context.getClass())) {
      return (WorkflowState) context;
    }
    throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
  }

}
