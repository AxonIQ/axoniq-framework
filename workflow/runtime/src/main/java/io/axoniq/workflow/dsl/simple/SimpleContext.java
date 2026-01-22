package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.StateManager;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import io.axoniq.workflow.runtime.context.WorkflowExecution;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

import java.time.Clock;
import java.util.Map;

public class SimpleContext extends WorkflowExecution
  implements WorkflowContext, WorkflowState, ExecuteInLocalContext, WaitForEvent {

  public SimpleContext(String workflowId, Map<String, Object> payload, StateManager stateManager, EventAppender eventAppender, Clock clock) {
    super(workflowId, payload, stateManager, eventAppender, clock);
  }
}
