package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.workflow.*;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

public interface OtherDefinition extends
  WorkflowDefinition<OtherWorkflowContext>,
  AssociationProvider,
  WorkflowConfiguration<OtherWorkflowContext> {

  String association(@NotNull Map<String, Object> payload);

  class TestWorkflowContextFactory implements WorkflowContextFactory<OtherWorkflowContext> {

    private final AssociationProvider associationProvider;

    TestWorkflowContextFactory(AssociationProvider associationProvider) {
      this.associationProvider = associationProvider;
    }

    @NotNull
    @Override
    public OtherWorkflowContext createContext(
      @NotNull Map<String, Object> initialPayload,
      @Nonnull WorkflowServices workflowServices) {
      var workflowId = associationProvider.associationKey(initialPayload).orElseThrow(() -> new IllegalStateException("Could not create workflow id"));
      return new OtherWorkflowContext(workflowId, initialPayload, workflowServices);
    }
  }

  @NotNull
  @Override
  default Optional<String> associationKey(@NotNull Map<String, Object> payload) {
    return Optional.of(association(payload));
  }


  @Override
  @Nonnull
  default WorkflowDefinition<OtherWorkflowContext> workflowDefinition() {
    return this;
  }

  @Override
  @Nonnull
  default AssociationProvider associationProvider() {
    return this;
  }

  @Override
  @Nonnull
  default WorkflowContextFactory<OtherWorkflowContext> workflowContextFactory() {
    return new TestWorkflowContextFactory(this);
  }

  @NotNull
  @Override
  default WorkflowStateFactory workflowStateFactory() {
    return new TestWorkflowStateFactory();
  }

  class TestWorkflowStateFactory implements WorkflowStateFactory {

    @Override
    public WorkflowState create(@NotNull WorkflowContext context) {
      if (context instanceof OtherWorkflowContext testWorkflowContext) {
        return testWorkflowContext;
      }
      throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
    }
  }
}
