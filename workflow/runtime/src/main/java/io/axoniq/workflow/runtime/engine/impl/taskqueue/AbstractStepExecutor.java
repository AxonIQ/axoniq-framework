package io.axoniq.workflow.runtime.engine.impl.taskqueue;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;

public abstract class AbstractStepExecutor {

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

  protected CompletableFuture<Void> started(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendEvent(startedStep(workflowContext, stepName, payload, eventNameCustomizer));
  }

  protected CompletableFuture<Void> completed(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    return sendEvent(completedStep(workflowContext, stepName, payload, eventNameCustomizer));
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
    return workflowServices.getEventSink().publish( null, eventMessage);
  }

}
