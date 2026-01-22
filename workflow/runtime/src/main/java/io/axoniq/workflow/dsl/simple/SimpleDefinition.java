package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.*;
import io.axoniq.workflow.runtime.engine.StateManager;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;

public interface SimpleDefinition extends
  WorkflowDefinition<SimpleContext>,
  WorkflowContextFactory<SimpleContext>,
  WorkflowStateFactory,
  AssociationProvider,
  WorkflowConfiguration<SimpleContext> {

  @Nonnull
  @Override
  default SimpleContext createContext(@Nonnull Map<String, Object> payload, @NotNull StateManager stateManager) {
    return new SimpleContext(
      associationKey(payload).orElseThrow(() -> new IllegalStateException("Could not extract correlation key from payload : " + payload)),
      payload,
      stateManager,
      Clock.systemDefaultZone()
    );
  }

  @Override
  default WorkflowState create(@NotNull WorkflowContext context) {
    if (!(context instanceof WorkflowState)) {
      throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
    }
    return (WorkflowState) context;
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

  @NotNull
  @Override
  default WorkflowStateFactory workflowLifecycleFactory() {
    return this;
  }

}
