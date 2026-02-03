package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.*;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

public interface TestDefinition extends
  WorkflowDefinition<TestWorkflowContext>,
  AssociationProvider,
  WorkflowConfiguration<TestWorkflowContext> {

  String association(@NotNull Map<String, Object> payload);

  class TestWorkflowContextFactory implements WorkflowContextFactory<TestWorkflowContext> {

    private final AssociationProvider associationProvider;

    TestWorkflowContextFactory(AssociationProvider associationProvider) {
      this.associationProvider = associationProvider;
    }

    @NotNull
    @Override
    public TestWorkflowContext createContext(
      @NotNull Map<String, Object> initialPayload,
      @Nonnull WorkflowServices workflowServices) {
      var workflowId = associationProvider.associationKey(initialPayload).orElseThrow(() -> new IllegalStateException("Could not create workflow id"));
      return new TestWorkflowContext(workflowId, initialPayload, workflowServices);
    }
  }

  @NotNull
  @Override
  default Optional<String> associationKey(@NotNull Map<String, Object> payload) {
    return Optional.of(association(payload));
  }


  @Override
  @Nonnull
  default WorkflowDefinition<TestWorkflowContext> workflowDefinition() {
    return this;
  }

  @Override
  @Nonnull
  default AssociationProvider associationProvider() {
    return this;
  }

  @Override
  @Nonnull
  default WorkflowContextFactory<TestWorkflowContext> workflowContextFactory() {
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
      if (context instanceof TestWorkflowContext testWorkflowContext) {
        return testWorkflowContext;
      }
      throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
    }
  }
}
