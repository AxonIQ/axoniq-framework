package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
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

  private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);

  private final StateManager stateManager;
  private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

  public WorkflowEngine() {
    this(new StateManager());
  }

  public WorkflowEngine(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  /**
   * Event Sourcing: Initialize, re-hydrate context state from history, then execute workflow.
   */
  public <T extends WorkflowContext> CompletableFuture<T> restoreAndExecute(WorkflowConfiguration<T> configuration, Map<String, Object> payload) {
    T context = initialize(configuration, payload);
    WorkflowState lifecycle = restore(configuration, context);
    return execute(configuration, context, lifecycle);
  }

  public <T extends WorkflowContext> CompletableFuture<T> restoreAndExecute(WorkflowConfiguration<T> configuration) {
    return restoreAndExecute(configuration, Map.of());
  }

  public <T extends WorkflowContext> T initialize(WorkflowConfiguration<T> configuration, Map<String, Object> payload) {
    return configuration.workflowContextFactory().createContext(payload, stateManager);
  }

  public <T extends WorkflowContext> T initialize(WorkflowConfiguration<T> configuration) {
    return configuration.workflowContextFactory().createContext(Map.of(), stateManager);
  }

  /**
   * Event Sourcing Hydration: Replays historical events to reconstruct workflow state.
   * Creates the context and applies all persisted events, restoring both step-level
   * and workflow-level state. Must be called before workflow execution to ensure
   * the context reflects the complete event history.
   */
  public <T extends WorkflowContext> WorkflowState restore(WorkflowConfiguration<T> configuration, T context) {
    var lifecycle = configuration.workflowLifecycleFactory().create(context);
    List<EventMessage> history = stateManager.getHistory(context.getWorkflowId());

    for (EventMessage event : history) {
      Object eventPayload = event.payloadAs(Object.class);
      var metadata = event.metadata();

      // Apply step-level state changes
      getStepStatus(metadata).ifPresent(stepStatus -> {
        var stepName = getStepName(metadata);
        switch (stepStatus) {
          case STARTED:
            lifecycle.addStep(StepExecution.started(stepName, eventPayload));
            break;
          case FAILED:
            lifecycle.addStep(StepExecution.failed(stepName, (Throwable) eventPayload));
            break;
          case TIMED_OUT:
            lifecycle.addStep(StepExecution.timedOut(stepName, eventPayload));
            break;
          case COMPLETED:
            lifecycle.addStep(StepExecution.completed(stepName, eventPayload));
            break;
          default:
            break;
        }
      });

      // Apply workflow-level state changes
      getWorkflowStatus(metadata).ifPresent(lifecycle::setStatus);
    }

    return lifecycle;
  }

  public <T extends WorkflowContext> CompletableFuture<T> execute(WorkflowConfiguration<T> configuration, T context, WorkflowState lifecycle) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        // Check if workflow is already in terminal state - do nothing
        if (context.getStatus().isTerminal()) {
          return context;
        }
        configuration.workflowDefinition().execute(context);

        lifecycle.setStatus(WorkflowStatus.COMPLETED);
        stateManager.append(completedWorkflow(context));

        return context;
      } catch (WorkflowFailedException e) {
        // User explicitly failed the workflow
        lifecycle.setStatus(WorkflowStatus.FAILED);
        stateManager.append(failedWorkflow(context, e));
        throw e;
      } catch (RuntimeException e) {
        // Any other runtime exception - log and rethrow, stay ACTIVE
        logger.warn("Workflow {} encountered error, staying active: {}", context.getWorkflowId(), e.getMessage());
        throw e;
      }
    }, virtualThreadExecutor);
  }

}
