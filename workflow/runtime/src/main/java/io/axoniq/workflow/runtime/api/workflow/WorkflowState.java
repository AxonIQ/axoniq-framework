package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.engine.StepExecution;
import io.axoniq.workflow.runtime.engine.WorkflowStatus;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

import java.time.Clock;

public interface WorkflowState {

  EventAppender eventAppender();

  Clock getClock();

  void modifyPayload(PayloadProcessor payloadModification);

  void addStep(StepExecution execution);

  StepExecution getStep(String stepName);

  void setStatus(WorkflowStatus status);

  StateManager stateManager();
}
