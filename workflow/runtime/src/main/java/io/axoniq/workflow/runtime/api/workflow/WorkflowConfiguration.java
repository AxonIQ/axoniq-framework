package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
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
  WorkflowStateFactory workflowStateFactory();

  @Nonnull
  AssociationProvider associationProvider();

  @Nonnull
  default EventNameCustomizer eventNameCustomizer() {
    return DefaultEventNameCustomizer.Builder.eventName();
  }
}
