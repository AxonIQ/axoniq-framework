package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.context.WorkflowExecutionImpl;
import io.axoniq.workflow.runtime.engine.StateManager;

import java.util.Map;

public class SimpleContext extends WorkflowExecutionImpl implements WorkflowContext, Execute, WaitForEvent {

  public SimpleContext(String workflowId, StateManager stateManager, Map<String, Object> payload) {
    super(workflowId, stateManager, payload);
  }
}
