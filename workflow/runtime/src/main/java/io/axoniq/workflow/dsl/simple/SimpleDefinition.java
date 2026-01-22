package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.AssociationProvider;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

public interface SimpleDefinition extends
  WorkflowDefinition<SimpleContext>,
  WorkflowContextFactory<SimpleContext>,
  AssociationProvider,
  WorkflowConfiguration<SimpleContext> {

  @Nonnull
  @Override
  default SimpleContext createContext(@Nonnull Map<String, Object> payload, @Nonnull StateManager stateManager) {
    return new SimpleContext(
      associationKey(payload).orElseThrow(() -> new IllegalStateException("Could not extract correlation key from payload : " + payload)),
      payload,
      stateManager
    );
  }

  @NotNull
  @Override
  default Optional<String> associationKey(@NotNull Map<String, Object> payload) {
    return Optional.of(association(payload));
  }

  String association(@NotNull Map<String, Object> payload);

  @Override
  @Nonnull
  default WorkflowDefinition<SimpleContext> workflowDefinition() {
    return this;
  }

  @Override
  @Nonnull
  default WorkflowContextFactory<SimpleContext> workflowContextFactory() {
    return this;
  }

  @Override
  @Nonnull
  default AssociationProvider associationProvider() {
    return this;
  }
}
