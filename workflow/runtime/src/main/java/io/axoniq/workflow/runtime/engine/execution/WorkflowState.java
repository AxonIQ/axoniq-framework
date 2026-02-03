package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Clock;
import java.util.function.Consumer;

/**
 * Internal looking API for the execution.
 */
public interface WorkflowState {

  void applyStateChange(EventMessage eventMessage);

  void runNextStateChange() throws InterruptedException;

  StepExecution getStep(String stepName);

  void onEvent(EventMessage eventMessage, ProcessingContext processingContext);

  void addStep(StepExecution stepExecution);

  boolean containsStep(String stepName);

  boolean appendTask(Consumer<WorkflowState> task);

  Consumer<WorkflowState> getNextTask();

  boolean isExecutable();

  boolean hasTasks();
}
