package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.util.ContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.Context;
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
      if (poll != null) { // FIXME
        poll.accept(this.workflowState);
      }
    }
  }

  protected Context getContext(String stepName) {
    return workflowState.containsStep(stepName)
      ? workflowState.getStep(stepName).context()
      : workflowState.processingContext();
  }

  protected CompletableFuture<Void> started(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(startedStep(workflowContext, stepName, sanitize(payload), eventNameCustomizer), getContext(stepName));
  }

  protected CompletableFuture<Void> completed(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(completedStep(workflowContext, stepName, sanitize(payload), eventNameCustomizer), getContext(stepName));
  }

  protected CompletableFuture<Void> cancelled(String stepName, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(cancelledStep(workflowContext, stepName, eventNameCustomizer), getContext(stepName));
  }

  protected CompletableFuture<Void> failed(String stepName, Throwable ex, EventNameCustomizer eventNameCustomizer) {
    LoggerFactory.getLogger(AbstractStepExecutor.class).error("Error", ex);
    return sendStepEvent(failStep(workflowContext, stepName, ex, eventNameCustomizer), getContext(stepName));
  }

  protected CompletableFuture<Void> timedOut(String stepName, EventNameCustomizer eventNameCustomizer) {
    return timedOut(stepName, Instant.now(workflowServices.getClock()), eventNameCustomizer);
  }

  protected CompletableFuture<Void> timedOut(String stepName, Instant timeoutTimestamp, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(timeoutStep(workflowContext, stepName, timeoutTimestamp, eventNameCustomizer), getContext(stepName));
  }

  private CompletableFuture<Void> sendStepEvent(EventMessage eventMessage, Context context) {
    logger.trace("Appending event {}", eventMessage.type());
    return workflowServices.getUnitOfWorkFactory().create().executeWithResult(
      processingContext -> workflowServices.getEventSink().publish(
        ContextUtils.copyResources(context, processingContext),
        eventMessage
      ));
  }

  private Map<String, Object> sanitize(Map<String, Object> payload) {
    if (payload == null) {
      return new LinkedHashMap<>();
    }
    return payload;
  }
}
