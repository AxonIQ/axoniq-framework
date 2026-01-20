package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepExecution;

import java.time.Clock;
import java.util.Map;
import java.util.Set;

public interface WorkflowContext extends
  ExecutePrimitive,
  WaitForPrimitive {

  String getWorkflowId();

  Map<String, Object> getPayload();

  StateManager getStateManager();

  void modifyPayload(PayloadFunction payloadModification);

  void addStep(String stepName, StepExecution execution);

  StepExecution getStep(String stepName);

  Set<String> getStepHistory();

  Clock getClock();

  void restoreStep(StepExecution stepExecution);
}
