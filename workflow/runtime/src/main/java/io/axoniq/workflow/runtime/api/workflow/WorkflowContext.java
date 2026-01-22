package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
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


  // TODO segregate to a different interface

  StateManager getStateManager();

  void modifyPayload(PayloadFunction payloadModification);

  void addStep(StepExecution execution);

  StepExecution getStep(String stepName);

  Set<String> getStepHistory();

  Clock getClock();

  void restoreStep(StepExecution stepExecution);

  void restoreCompleted();

  void restoreFailed();

}
