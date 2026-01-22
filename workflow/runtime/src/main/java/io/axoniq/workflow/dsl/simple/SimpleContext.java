package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import io.axoniq.workflow.runtime.context.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.StateManager;

import java.time.Clock;
import java.util.Map;
import java.util.function.Supplier;

public class SimpleContext extends WorkflowExecution implements WorkflowContext, WorkflowState, ExecuteInLocalContext, WaitForEvent {

  public SimpleContext(String workflowId, Map<String, Object> payload, StateManager stateManager, Clock clock) {
    super(workflowId, payload, stateManager, clock);
  }
}
