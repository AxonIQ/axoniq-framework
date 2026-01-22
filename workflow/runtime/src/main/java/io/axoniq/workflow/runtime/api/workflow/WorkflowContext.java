package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepExecution;
import io.axoniq.workflow.runtime.engine.WorkflowStatus;

import java.time.Clock;
import java.util.Map;
import java.util.Set;

public interface WorkflowContext extends
  ExecutePrimitive,
  WaitForPrimitive {

  String getWorkflowId();

  Map<String, Object> getPayload();

  WorkflowStatus getStatus();

  // TODO segregate to a different interface

  StateManager getStateManager();

  void modifyPayload(PayloadProcessor payloadModification);

  void addStep(StepExecution execution);

  StepExecution getStep(String stepName);

  Set<String> getStepHistory();

  Clock getClock();

  void restoreStep(StepExecution stepExecution);

  void setStatus(WorkflowStatus status);
}
