package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.context.WorkflowExecutionImpl;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.completedWorkflow;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.failedWorkflow;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepName;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus;

public class WorkflowEngine {

  // TODO: recover the WF final states
  public static final String WF_STARTED = "io.axoniq.workflow.WorkflowStarted#0.1";
  public static final String WF_FAILED = "io.axoniq.workflow.WorkflowFailed#0.1";
  public static final String WF_COMPLETED = "io.axoniq.workflow.WorkflowCompleted#0.1";

  private final StateManager stateManager;
  private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

  public WorkflowEngine() {
    this(new StateManager());
  }

  public WorkflowEngine(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  private static boolean isRecoverableException(Throwable e) {
    // Check for InterruptedException (including wrapped)
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof InterruptedException) {
        return true;
      }
      cause = cause.getCause();
    }
    return e instanceof CancellationException;
  }

  public <T extends WorkflowContext> CompletableFuture<T> execute(WorkflowDefinition<T> definition) {
    return execute(definition, Map.of());
  }

  public <T extends WorkflowContext> CompletableFuture<T> execute(WorkflowDefinition<T> definition, Map<String, Object> workflowPayload) {
    // 2. Create context
    T context = definition.createContext(this.stateManager, workflowPayload);

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

    // Restore workflow-level status from history
    for (EventMessage event : history) {
      String eventTypeName = event.type().qualifiedName().toString();
      if (eventTypeName.endsWith("Completed#0.1")) {
        ((WorkflowExecutionImpl) context).restoreCompleted();
      } else if (eventTypeName.endsWith("Failed#0.1")) {
        ((WorkflowExecutionImpl) context).restoreFailed();
      }
    }

    return CompletableFuture.supplyAsync(() -> {
      try {
       //if failed  or completed or cancelled or timedout do nothing. (all terminal)
        definition.execute(context);
        //separate event sourced from our state
        //context.setStatus(COMPLETED)
        stateManager.append(completedWorkflow(context));
        return context;
      } catch (RuntimeException e) {
        if (isRecoverableException(e)) { //on runtime we stay active -> on WorkflowFailedExecption FAIL.
          // Keep ACTIVE - can recover later
          //fail((e)- throw new WorkflowFailedException(e))
          throw e;
        }
        stateManager.append(failedWorkflow(context, e));
        throw e;
      } catch (Error e) {
        // JVM errors - rethrow without marking FAILED
        throw e;
      }
    }, virtualThreadExecutor);
  }
}
