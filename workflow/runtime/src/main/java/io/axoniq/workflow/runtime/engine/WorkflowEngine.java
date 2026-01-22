package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;
import static io.axoniq.workflow.runtime.util.MetadataUtils.*;

public class WorkflowEngine {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);

  private final StateManager stateManager;
  private final EventAppender eventAppender;
  private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

  public WorkflowEngine(
    @Nonnull StateManager stateManager
  ) {
    this.stateManager = stateManager;
    this.eventAppender = stateManager;
  }

  /**
   * Event Sourcing: Initialize, re-hydrate context state from history, then execute workflow.
   */
  public <T extends WorkflowContext> CompletableFuture<T> restoreAndExecute(WorkflowConfiguration<T> configuration, Map<String, Object> payload) {
    T context = initialize(configuration, payload);
    WorkflowState state = restore(configuration, context);
    return execute(configuration, context, state);
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
    var state = configuration.workflowStateFactory().create(context);
    List<EventMessage> history = stateManager.getHistory(context.getWorkflowId());

    for (EventMessage event : history) {
      Object eventPayload = event.payloadAs(Object.class);
      var metadata = event.metadata();

      // Apply step-level state changes
      getStepStatus(metadata).ifPresent(stepStatus -> {
        var stepName = getStepName(metadata);
        switch (stepStatus) {
          case STARTED:
            state.addStep(StepExecution.started(stepName, eventPayload));
            break;
          case FAILED:
            state.addStep(StepExecution.failed(stepName, (Throwable) eventPayload));
            break;
          case TIMED_OUT:
            state.addStep(StepExecution.timedOut(stepName, eventPayload));
            break;
          case COMPLETED:
            state.addStep(StepExecution.completed(stepName, eventPayload));
            break;
          default:
            break;
        }
      });

      // Apply workflow-level state changes
      getWorkflowStatus(metadata).ifPresent(state::setStatus);
    }

    return state;
  }

  public <T extends WorkflowContext> CompletableFuture<T> execute(WorkflowConfiguration<T> configuration, T context, WorkflowState state) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        // Check if workflow is already in terminal state - do nothing
        if (context.getStatus().isTerminal()) {
          return context;
        }

        state.setStatus(WorkflowStatus.STARTED);
        eventAppender.append(startedWorkflow(context));

        configuration.workflowDefinition().execute(context);

        state.setStatus(WorkflowStatus.COMPLETED);
        eventAppender.append(completedWorkflow(context));

        return context;
      } catch (WorkflowFailedException e) {
        // User explicitly failed the workflow
        state.setStatus(WorkflowStatus.FAILED);
        eventAppender.append(failedWorkflow(context, e));

        throw e;
      } catch (RuntimeException e) {
        // Any other runtime exception - log and rethrow, stay ACTIVE
        logger.warn("Workflow {} encountered error, staying active: {}", context.getWorkflowId(), e.getMessage());
        throw e;
      }
    }, virtualThreadExecutor);
  }
}
