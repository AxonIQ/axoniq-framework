package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.AssociationProvider;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.execution.ContextToStateAdoptingStateFactory;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

public interface MyWorkflowDefinition extends
  WorkflowDefinition<MyWorkflowContext>,
  WorkflowConfiguration<MyWorkflowContext> {

  String association(@NotNull Map<String, Object> payload);

  void execute(@Nonnull MyWorkflowContext context);

  @Override
  @Nonnull
  default WorkflowDefinition<MyWorkflowContext> workflowDefinition() {
    return this::execute;
  }

  @Override
  default void accept(MyWorkflowContext myWorkflowContext) {
    this.execute(myWorkflowContext);
  };

  @Override
  @Nonnull
  default AssociationProvider associationProvider() {
    return payload -> Optional.ofNullable(association(payload));
  }

  @Override
  @Nonnull
  default WorkflowContextFactory<MyWorkflowContext> workflowContextFactory() {
    return new MyWorkflowContextFactory(eventNameCustomizer());
  }

  @NotNull
  @Override
  default WorkflowStateFactory workflowStateFactory() {
    return new ContextToStateAdoptingStateFactory<>(MyWorkflowContext.class);
  }

}
