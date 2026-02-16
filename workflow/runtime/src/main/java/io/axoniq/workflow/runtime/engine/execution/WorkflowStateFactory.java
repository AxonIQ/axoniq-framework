package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

@Internal
@FunctionalInterface
public interface WorkflowStateFactory {
  /**
   * Creates state for given workflow context.
   * @param context context to create the workflow state for.
   * @return workflow state.
   */
  @Nonnull
  WorkflowState create(@Nonnull WorkflowContext context);
}
