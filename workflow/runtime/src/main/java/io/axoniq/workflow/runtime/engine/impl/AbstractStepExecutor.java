package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowServices;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;

public abstract class AbstractStepExecutor {

  private static final Logger logger = LoggerFactory.getLogger(AbstractStepExecutor.class);
  protected final WorkflowContext workflowContext;
  protected final WorkflowState workflowState;
  protected final WorkflowServices workflowServices;
  protected final EventNameCustomizer parentEventNameCustomizer;

  public AbstractStepExecutor(
    @Nonnull WorkflowContext workflowContext,
    @Nonnull WorkflowState workflowState,
    @Nonnull WorkflowServices workflowServices,
    @Nonnull EventNameCustomizer parentEventNameCustomizer
  ) {
    this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
    this.workflowState = Objects.requireNonNull(workflowState, "Workflow state is mandatory");
    this.workflowServices = Objects.requireNonNull(workflowServices, "Workflow services are mandatory");
    this.parentEventNameCustomizer = Objects.requireNonNull(parentEventNameCustomizer, "Event name customizer is mandatory");
  }

  protected void acceptAllPendingTasksForStep(String stepName) {
    while ((!workflowState.containsStep(stepName) && !workflowState.hasTasks()) || !workflowState.isExecutable()) {
      var poll = workflowState.getNextTask();
      if (poll != null) { // FIXME forever?
        poll.accept(this.workflowState);
      }
    }
  }

  protected Context getContext(@Nonnull String stepName) {
    return workflowState.containsStep(stepName)
      ? workflowState.getStep(stepName).context()
      : workflowState.processingContext();
  }

  protected CompletableFuture<Void> started(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(startedStep(workflowContext, stepName, sanitize(payload),
      merge(parentEventNameCustomizer, eventNameCustomizer)
    ), getContext(stepName));
  }

  protected CompletableFuture<Void> completed(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(completedStep(workflowContext, stepName, sanitize(payload),
      merge(parentEventNameCustomizer, eventNameCustomizer)
    ), getContext(stepName));
  }

  protected CompletableFuture<Void> cancelled(String stepName, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(cancelledStep(workflowContext, stepName,
      merge(parentEventNameCustomizer, eventNameCustomizer)
    ), getContext(stepName));
  }

  protected CompletableFuture<Void> failed(String stepName, Throwable ex, EventNameCustomizer eventNameCustomizer) {
    LoggerFactory.getLogger(AbstractStepExecutor.class).error("Error", ex);
    return sendStepEvent(failStep(workflowContext, stepName, ex,
      merge(parentEventNameCustomizer, eventNameCustomizer)
    ), getContext(stepName));
  }

  protected CompletableFuture<Void> timedOut(String stepName, EventNameCustomizer eventNameCustomizer) {
    return timedOut(stepName, Instant.now(workflowServices.getClock()), eventNameCustomizer);
  }

  protected CompletableFuture<Void> timedOut(String stepName, Instant timeoutTimestamp, EventNameCustomizer eventNameCustomizer) {
    return sendStepEvent(timeoutStep(workflowContext, stepName, timeoutTimestamp,
      merge(parentEventNameCustomizer, eventNameCustomizer)
    ), getContext(stepName));
  }

  private CompletableFuture<Void> sendStepEvent(EventMessage eventMessage, Context context) {
    logger.trace("Appending event {}", eventMessage.type());
    return ContextUtils.executeWithResult(
      null,
      workflowServices,
      context,
      ctx -> workflowServices.getEventSink().publish(ctx, eventMessage)
    );
  }

  private Map<String, Object> sanitize(Map<String, Object> payload) {
    if (payload == null) {
      return new LinkedHashMap<>();
    }
    return payload;
  }
}
