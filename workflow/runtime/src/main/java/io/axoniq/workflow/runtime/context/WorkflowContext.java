package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.context.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.context.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.payload.Payload;
import io.axoniq.workflow.runtime.payload.PayloadFunction;

public interface WorkflowContext extends
  ExecutePrimitive,
  WaitForPrimitive {

  String getWorkflowId();

  Payload getPayload();

  StateManager getStateManager();

  void modifyPayload(PayloadFunction payloadModification);
}
