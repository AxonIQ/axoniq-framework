package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.CorrelationProvider;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import jakarta.annotation.Nonnull;

/**
 * Configures definition, context factory and correlation provider.
 *
 * @param <T> workflow context type.
 */
public interface WorkflowConfiguration<T extends WorkflowContext> {

  @Nonnull
  WorkflowDefinition<T> workflowDefinition();

  @Nonnull
  WorkflowContextFactory<T> workflowContextFactory();

  @Nonnull
  CorrelationProvider correlatorProvider();
}
