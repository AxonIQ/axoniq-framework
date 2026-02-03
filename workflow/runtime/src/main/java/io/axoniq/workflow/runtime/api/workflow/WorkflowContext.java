package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;

import java.util.List;
import java.util.Map;

/**
 * Public API facing class to access the execution from workflow definition.
 */
public interface WorkflowContext extends
  ExecutePrimitive,
  WaitForPrimitive {

  String getWorkflowId();

  Map<String, Object> getPayload();

  WorkflowStatus getStatus();

  List<String> getStepHistory();
}
