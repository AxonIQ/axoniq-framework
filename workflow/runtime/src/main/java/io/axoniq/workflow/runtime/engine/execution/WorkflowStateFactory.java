package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

@Internal
public interface WorkflowStateFactory {
  /**
   * Creates state for given workflow context.
   * @param context context to create the workflow state for.
   * @return workflow state.
   */
  WorkflowState create(@Nonnull WorkflowContext context);
}
