package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.context.WorkflowExecutionImpl;
import io.axoniq.workflow.runtime.exception.WorkflowFailedException;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.completedWorkflow;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.failedWorkflow;
import static io.axoniq.workflow.runtime.util.MetadataUtils.*;

public class WorkflowEngine {

  private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

  private final StateManager stateManager;
  private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

  public WorkflowEngine() {
    this(new StateManager());
  }

  public WorkflowEngine(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  public <T extends WorkflowContext> CompletableFuture<T> execute(WorkflowConfiguration<T> configuration) {
    return execute(configuration, apply(configuration, Map.of()));
  }

  public <T extends WorkflowContext> CompletableFuture<T> execute(WorkflowConfiguration<T> configuration, T context) {
    return CompletableFuture.supplyAsync(() -> {
      var impl = (WorkflowExecutionImpl) context;
      try {
        // Check if workflow is already in terminal state - do nothing
        if (impl.getStatus().isTerminal()) {
          return context;
        }

        configuration.workflowDefinition().execute(context);

        impl.setStatus(WorkflowStatus.COMPLETED);
        stateManager.append(completedWorkflow(context));
        return context;
      } catch (WorkflowFailedException e) {
        // User explicitly failed the workflow
        impl.setStatus(WorkflowStatus.FAILED);
        stateManager.append(failedWorkflow(context, e));
        throw e;
      } catch (RuntimeException e) {
        // Any other runtime exception - log and rethrow, stay ACTIVE
        log.warn("Workflow {} encountered error, staying active: {}",
          context.getWorkflowId(), e.getMessage());
        throw e;
      }
    }, virtualThreadExecutor);
  }

  /**
   * Event Sourcing Hydration: Replays historical events to reconstruct workflow state.
   * Creates the context and applies all persisted events, restoring both step-level
   * and workflow-level state. Must be called before workflow execution to ensure
   * the context reflects the complete event history.
   */
  public <T extends WorkflowContext> T apply(WorkflowConfiguration<T> configuration, Map<String, Object> workflowPayload) {
    T context = configuration.workflowContextFactory().createContext(workflowPayload, this.stateManager);
    List<EventMessage> history = stateManager.getHistory(context.getWorkflowId());

    for (EventMessage event : history) {
      Object eventPayload = event.payloadAs(Object.class);
      var metadata = event.metadata();

      // Apply step-level state changes
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

      // Apply workflow-level state changes
      getWorkflowStatus(metadata).ifPresent(workflowStatus -> {
        if (workflowStatus == WorkflowStatus.COMPLETED) {
          context.restoreCompleted();
        } else if (workflowStatus == WorkflowStatus.FAILED) {
          context.restoreFailed();
        }
      });
    }

    return context;
  }
}
