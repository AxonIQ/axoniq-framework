package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.context.StepExecution;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.definition.WorkflowDefinition;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepFailed;
import io.axoniq.workflow.runtime.event.StepStarted;
import io.axoniq.workflow.runtime.payload.Payload;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.util.List;

public class WorkflowEngine {

  public static final String WF_STARTED = "io.axoniq.workflow.WorkflowStarted#0.1";
  public static final String WF_FAILED = "io.axoniq.workflow.WorkflowFailed#0.1";
  public static final String WF_COMPLETED = "io.axoniq.workflow.WorkflowCompleted#0.1";

  private final StateManager stateManager;
  public WorkflowContextImpl context = null;

  public WorkflowEngine() {
    this(new StateManager());
  }

  public WorkflowEngine(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  public void execute(String workflowId, WorkflowDefinition definition) {
    execute(workflowId, definition, Payload.empty());
  }

  public void execute(String workflowId, WorkflowDefinition definition, Payload workflowPayload) {
    // 2. Create context
    context = new WorkflowContextImpl(workflowId, this.stateManager, workflowPayload);

    // 3. Apply history events to context
    List<EventMessage> history = stateManager.getHistory(workflowId);
    for (EventMessage event : history) {
      Object eventPayload = event.payloadAs(Object.class);
      if (eventPayload instanceof StepStarted s) {
        context.restoreStep(StepExecution.inProgress(s.stepId(), event.timestamp()));
      } else if (eventPayload instanceof StepCompleted s) {
        context.restoreStep(StepExecution.completed(s.stepId(), s.result()));
      } else if (eventPayload instanceof StepFailed s) {
        context.restoreStep(StepExecution.failed(s.stepId(), s.cause()));
      }
    }

    // 4. Execute workflow
    try {
      definition.execute(context);
    } catch (RuntimeException e) {
      var failed = new GenericEventMessage(MessageType.fromString(WF_FAILED), new StepFailed(workflowId, e.getMessage(), e));
      stateManager.append(workflowId, failed);
    }

  }
}
