package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.beans.PropertyEditor;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Internal looking API for the execution.
 */
public interface WorkflowState {

  <T extends WorkflowContext> T execute(
    WorkflowConfiguration<T> configuration,
    WorkflowContext workflowContext
  ) throws ExecutionSuspended;

  void applyStateChange(EventMessage eventMessage, ProcessingContext processingContext);

  void awaitStateChange(Predicate<WorkflowState> condition) throws InterruptedException;

  StepExecution getStep(String stepName);

  void onEvent(EventMessage eventMessage, ProcessingContext processingContext);

  void addStep(StepExecution stepExecution);

  boolean containsStep(String stepName);

  void appendTask(Consumer<WorkflowState> task);

  Consumer<WorkflowState> getNextTask();

  WorkflowStatus getStatus();

  boolean isExecutable();

  boolean hasTasks();

  void registerWaitCondition(String stepName, QualifiedName qualifiedName, Predicate<EventMessage> predicate, EventNameCustomizer eventNameCustomizer);

  void removeWaitCondition(String stepName);

  // FIXME check if we can replace this for the Context interface
  ProcessingContext processingContext();
}
