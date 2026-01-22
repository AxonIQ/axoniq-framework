package io.axoniq.workflow.runtime.api.workflow;

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
  AssociationProvider associationProvider();
}
