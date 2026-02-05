package io.axoniq.workflow.runtime.engine.impl.taskqueue;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.StepExecutionResults;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;

public abstract class AbstractStepExecutor {

  private static final Logger logger = LoggerFactory.getLogger(AbstractStepExecutor.class);
  protected final WorkflowContext workflowContext;
  protected final WorkflowState workflowState;
  protected final WorkflowServices workflowServices;

  public AbstractStepExecutor(
    @Nonnull WorkflowContext workflowContext,
    @Nonnull WorkflowState workflowState,
    @Nonnull WorkflowServices workflowServices
  ) {
    this.workflowContext = workflowContext;
    this.workflowState = workflowState;
    this.workflowServices = workflowServices;
  }

  protected void acceptAllPendingTasksForStep(String stepName) {
    while ((!workflowState.containsStep(stepName) && !workflowState.hasTasks()) || !workflowState.isExecutable()) {
      var poll = workflowState.getNextTask();
      if (poll != null) {
        poll.accept(this.workflowState);
      }
    }
  }

  protected CompletableFuture<Void> started(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendEvent(startedStep(workflowContext, stepName, sanitize(payload), eventNameCustomizer));
  }

  protected CompletableFuture<Void> completed(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendEvent(completedStep(workflowContext, stepName, sanitize(payload), eventNameCustomizer));
  }

  protected CompletableFuture<Void> cancelled(String stepName, EventNameCustomizer eventNameCustomizer) {
    return sendEvent(cancelledStep(workflowContext, stepName, eventNameCustomizer));
  }

  protected CompletableFuture<Void> failed(String stepName, Throwable ex, EventNameCustomizer eventNameCustomizer) {
    return sendEvent(failStep(workflowContext, stepName, ex, eventNameCustomizer));
  }

  protected CompletableFuture<Void> timedOut(String stepName, EventNameCustomizer eventNameCustomizer) {
    return timedOut(stepName, Instant.now(workflowServices.getClock()), eventNameCustomizer);
  }

  protected CompletableFuture<Void> timedOut(String stepName, Instant timeoutTimestamp, EventNameCustomizer eventNameCustomizer) {
    return sendEvent(timeoutStep(workflowContext, stepName, timeoutTimestamp, eventNameCustomizer));
  }

  private CompletableFuture<Void> sendEvent(EventMessage eventMessage) {
    // FIXME -> processing context? uof?
    logger.trace("Appending event {}", eventMessage.type());
    return workflowServices.getEventSink().publish(null, eventMessage);
  }

  private Map<String, Object> sanitize(Map<String, Object> payload) {
    if (payload == null) {
      return new LinkedHashMap<>();
    }
    return payload;
  }
}
