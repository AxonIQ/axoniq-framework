package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
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
  AssociationProvider associationProvider();

  @Nonnull
  default String workflowName() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  default EventNameCustomizer eventNameCustomizer() {
    return DefaultEventNameCustomizer.Builder.eventName();
  }

  @Nonnull
  WorkflowContextFactory<T> workflowContextFactory();

  @Nonnull
  WorkflowStateFactory workflowStateFactory();

}
