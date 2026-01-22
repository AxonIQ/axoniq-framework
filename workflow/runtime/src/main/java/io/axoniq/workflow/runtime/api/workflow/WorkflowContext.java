package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.WorkflowStatus;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface WorkflowContext extends
  ExecutePrimitive,
  WaitForPrimitive {

  String getWorkflowId();

  Map<String, Object> getPayload();

  WorkflowStatus getStatus();

  Set<String> getStepHistory();

  void registerStatusChangeListener(Consumer<WorkflowStatus> workflowStateConsumer);

}
