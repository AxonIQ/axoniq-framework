package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Internal looking API for the execution.
 */
public interface WorkflowState {

  @Nonnull
  <T extends WorkflowContext> T execute(@Nonnull WorkflowConfiguration<T> workflowConfiguration, @Nonnull WorkflowContext workflowContext) throws ExecutionSuspended;

  void applyStateChange(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

  void awaitStateChange(@Nonnull Predicate<WorkflowState> condition) throws InterruptedException;

  @Nonnull
  WorkflowStep getStep(@Nonnull String stepName);

  void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

  void addStep(@Nonnull WorkflowStep workflowStep);

  boolean containsStep(@Nonnull String stepName);

  void appendTask(@Nonnull Consumer<WorkflowState> task);

  @Nullable
  Consumer<WorkflowState> getNextTask();

  @Nonnull
  WorkflowStatus getStatus();

  boolean isExecutable();

  boolean hasTasks();

  void registerWaitCondition(@Nonnull String stepName, @Nonnull QualifiedName qualifiedName, @Nonnull Predicate<EventMessage> predicate, @Nonnull EventNameCustomizer eventNameCustomizer);

  void removeWaitCondition(@Nonnull String stepName);

  // FIXME check if we can replace this for the Context interface
  @Nonnull
  ProcessingContext processingContext();

}
