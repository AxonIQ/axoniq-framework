package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.engine.StateManager;

import java.util.Map;

public class SimpleContext extends WorkflowContextImpl implements Execute, WaitForEvent {
  public SimpleContext(String workflowId, StateManager stateManager, Map<String, Object> payload) {
    super(workflowId, stateManager, payload);
  }
}
