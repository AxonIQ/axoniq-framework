package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Clock;

/**
 * Internal looking API for the execution.
 */
public interface WorkflowState {

  Clock getClock();

  void applyPayloadModification(PayloadProcessor payloadModification);

  StepExecution getStep(String stepName);

  /**
   * Mutate itself based on a new message.
   * @param eventMessage message mutating the workflow state.
   */
  void onEvent(EventMessage eventMessage, ProcessingContext processingContext);
}
