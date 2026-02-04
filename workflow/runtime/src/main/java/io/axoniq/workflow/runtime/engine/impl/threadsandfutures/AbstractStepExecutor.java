package io.axoniq.workflow.runtime.engine.impl.threadsandfutures;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Instant;
import java.util.Map;

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

  protected void started(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    sendEvent(startedStep(workflowContext, stepName, payload, eventNameCustomizer));
  }

  protected void completed(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    sendEvent(completedStep(workflowContext, stepName, payload, eventNameCustomizer));
  }

  protected void failed(String stepName, Throwable ex, EventNameCustomizer eventNameCustomizer) {
    sendEvent(failStep(workflowContext, stepName, ex, eventNameCustomizer));
  }

  protected void timedOut(String stepName, EventNameCustomizer eventNameCustomizer) {
    timedOut(stepName, Instant.now(workflowServices.getClock()), eventNameCustomizer);
  }

  protected void timedOut(String stepName, Instant timeoutTimestamp, EventNameCustomizer eventNameCustomizer) {
    sendEvent(timeoutStep(workflowContext, stepName, timeoutTimestamp, eventNameCustomizer));
  }

  private void sendEvent(EventMessage eventMessage) {
    // block the thread
    var sent = workflowServices.getWorkflowEventAppender().appendEvent(eventMessage, null);
    sent.join();
  }

}
