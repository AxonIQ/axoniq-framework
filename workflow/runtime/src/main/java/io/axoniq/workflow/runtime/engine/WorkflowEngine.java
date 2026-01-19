package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.definition.Result;
import io.axoniq.workflow.runtime.definition.WorkflowDefinition;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepFailed;
import io.axoniq.workflow.runtime.event.StepStarted;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

public class WorkflowEngine {

  public static final String WF_STARTED = "io.axoniq.workflow.WorkflowStarted#0.1";
  public static final String WF_FAILED = "io.axoniq.workflow.WorkflowFailed#0.1";
  public static final String WF_COMPLETED = "io.axoniq.workflow.WorkflowCompleted#0.1";

  private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);


  private final Queue<EventMessage> events = new ConcurrentLinkedQueue<>();
  private final StateManager stateManager;


  public WorkflowEngine() {
    this(new StateManager());
  }

  public WorkflowEngine(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  public void execute(WorkflowDefinition workflowDefinition) throws ExecutionException, InterruptedException {

    var workflowInstance = workflowDefinition.create();
    var instanceId = workflowInstance.getWorkflowId();
    var historyEvents = stateManager.getHistory(instanceId);
    if (historyEvents != null && !historyEvents.isEmpty()) {
      this.events.addAll(historyEvents);
    } else {
      var workflowStarted = new GenericEventMessage(MessageType.fromString(WF_STARTED), new StepStarted(instanceId));
      this.events.add(workflowStarted);
      stateManager.append(workflowInstance.getWorkflowId(), workflowStarted);
    }

    CompletableFuture<Result> executionResult;
    do {
      while (!events.isEmpty()) {
        EventMessage event = events.poll();
        workflowInstance = workflowInstance.apply(event);
      }

      executionResult = workflowInstance.execute(eventMessages -> {
        events.addAll(eventMessages);
        stateManager.appendAll(instanceId, eventMessages);
        return CompletableFuture.completedFuture(null);
      });

      long timeout = executionResult.get().timeout();
      if (events.isEmpty() && timeout > 0) {
        logger.info("Waiting for events or expiry of timeout: {}ms", timeout);
        Thread.sleep(timeout);
      }
    } while (!executionResult.get().isCompleted());

    var result = executionResult.get();
    if (result.error().isPresent()) {
      var error = result.error().get();
      logger.error("Error executing workflow", error);
      var failed = new GenericEventMessage(MessageType.fromString(WF_FAILED), new StepFailed(workflowInstance.getWorkflowId(), error.getMessage(), error));
      stateManager.append(workflowInstance.getWorkflowId(), failed);
    } else {
      logger.info("Successfully executed workflow.");
      var completed = new GenericEventMessage(MessageType.fromString(WF_COMPLETED), new StepCompleted(workflowInstance.getWorkflowId(), Map.of()));
      stateManager.append(workflowInstance.getWorkflowId(), completed);
    }
  }
}
