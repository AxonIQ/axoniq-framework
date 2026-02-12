package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.*;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

public interface OtherDefinition extends
  WorkflowDefinition<OtherWorkflowContext>,
  WorkflowConfiguration<OtherWorkflowContext> {

  String association(@NotNull Map<String, Object> payload);

  class TestWorkflowContextFactory implements WorkflowContextFactory<OtherWorkflowContext> {

    private final AssociationProvider associationProvider;
    private final EventNameCustomizer parentCustomizer;

    TestWorkflowContextFactory(
      @Nonnull AssociationProvider associationProvider,
      @Nonnull EventNameCustomizer parentCustomizer
    ) {
      this.associationProvider = associationProvider;
      this.parentCustomizer = parentCustomizer;
    }

    @NotNull
    @Override
    public OtherWorkflowContext createContext(
      @NotNull Map<String, Object> initialPayload,
      @Nonnull ProcessingContext processingContext,
      @Nonnull WorkflowServices workflowServices) {
      var workflowId = associationProvider.associationKey(initialPayload)
        .orElseThrow(() -> new IllegalStateException("Could not create workflow id"));
      return new OtherWorkflowContext(
        workflowId,
        initialPayload,
        processingContext,
        parentCustomizer,
        workflowServices);
    }
  }

  @Override
  @Nonnull
  default WorkflowDefinition<OtherWorkflowContext> workflowDefinition() {
    return this;
  }

  @Override
  @Nonnull
  default AssociationProvider associationProvider() {
    return new AssociationProvider() {
      @NotNull
      @Override
      public Optional<String> associationKey(@NotNull Map<String, Object> payload) {
        return Optional.of(association(payload));
      }
    };
  }

  @Override
  @Nonnull
  default WorkflowContextFactory<OtherWorkflowContext> workflowContextFactory() {
    return new TestWorkflowContextFactory(associationProvider(), eventNameCustomizer());
  }

  @NotNull
  @Override
  default WorkflowStateFactory workflowStateFactory() {
    return new TestWorkflowStateFactory();
  }

  class TestWorkflowStateFactory implements WorkflowStateFactory {

    @Override
    @Nonnull
    public WorkflowState create(@NotNull WorkflowContext context) {
      if (context instanceof OtherWorkflowContext testWorkflowContext) {
        return testWorkflowContext;
      }
      throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
    }
  }
}
