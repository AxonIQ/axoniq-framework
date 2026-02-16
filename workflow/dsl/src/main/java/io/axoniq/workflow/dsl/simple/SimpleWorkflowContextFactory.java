package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowServices;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class SimpleWorkflowContextFactory implements WorkflowContextFactory<SimpleWorkflowContext> {

  private final EventNameCustomizer parentCustomizer;

  public SimpleWorkflowContextFactory(
    @Nonnull EventNameCustomizer parentCustomizer
  ) {
    this.parentCustomizer = parentCustomizer;
  }

  @NotNull
  @Override
  public SimpleWorkflowContext createContext(
    @NotNull Map<String, Object> initialPayload,
    @Nonnull String workflowId,
    @Nonnull ProcessingContext processingContext,
    @Nonnull WorkflowServices workflowServices) {
    return new SimpleWorkflowContext(
      workflowId,
      initialPayload,
      processingContext,
      parentCustomizer,
      workflowServices);
  }
}
