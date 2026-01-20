package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.failedWorkflow;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepName;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus;

public class WorkflowEngine {

  // TODO: recover the WF final states
  public static final String WF_STARTED = "io.axoniq.workflow.WorkflowStarted#0.1";
  public static final String WF_FAILED = "io.axoniq.workflow.WorkflowFailed#0.1";
  public static final String WF_COMPLETED = "io.axoniq.workflow.WorkflowCompleted#0.1";

  private final StateManager stateManager;

  public WorkflowEngine() {
    this(new StateManager());
  }

  public WorkflowEngine(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  public <T extends WorkflowContext> T execute(String id, WorkflowDefinition<T> definition) {
    return execute(id, definition, Map.of());
  }

  public <T extends WorkflowContext> T execute(String id, WorkflowDefinition<T> definition, Map<String, Object> workflowPayload) {
    // 2. Create context
    T context = definition.createContext(id, this.stateManager, workflowPayload);

    // 3. Apply history events to context
    List<EventMessage> history = stateManager.getHistory(context.getWorkflowId());
    for (EventMessage event : history) {
      Object eventPayload = event.payloadAs(Object.class);
      var metadata = event.metadata();
      getStepStatus(metadata).ifPresent(stepStatus -> {
        var stepName = getStepName(metadata);
        switch (stepStatus) {
          case STARTED:
            context.restoreStep(StepExecution.started(stepName, eventPayload));
            break;
          case FAILED:
            context.restoreStep(StepExecution.failed(stepName, (Throwable) eventPayload));
            break;
          case TIMED_OUT:
            context.restoreStep(StepExecution.timedOut(stepName, eventPayload));
            break;
          case COMPLETED:
            context.restoreStep(StepExecution.completed(stepName, eventPayload));
            break;
          default:
            break;
        }
      });
    }

    // 4. Execute workflow
    try {
      definition.execute(context);
    } catch (RuntimeException e) {
      stateManager.append(failedWorkflow(context, e));
    }

    return context;
  }
}
