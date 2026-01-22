package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import io.axoniq.workflow.runtime.engine.StepExecution;
import jakarta.annotation.Nonnull;

import java.time.Instant;
import java.util.Map;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;

public abstract class AbstractPrimitiveDelegate {

  protected final WorkflowContext context;
  protected final WorkflowState state;

  public AbstractPrimitiveDelegate(@Nonnull WorkflowContext context, @Nonnull WorkflowState state) {
    this.context = context;
    this.state = state;
  }

  protected void started(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    state.addStep(StepExecution.started(stepName, Instant.now(state.getClock())));
    state.eventAppender().append(startedStep(context, stepName, payload, eventNameCustomizer));
  }

  protected void completed(String stepName, Map<String, Object> payload, EventNameCustomizer eventNameCustomizer) {
    state.addStep(StepExecution.completed(stepName, payload));
    state.eventAppender().append(completedStep(context, stepName, payload, eventNameCustomizer));
  }

  protected void failed(String stepName, Throwable ex, EventNameCustomizer eventNameCustomizer) {
    state.addStep(StepExecution.failed(stepName, ex));
    state.eventAppender().append(failStep(context, stepName, ex, eventNameCustomizer));
  }

  protected void timedOut(String stepName, EventNameCustomizer eventNameCustomizer) {
    var timeoutTimestamp = Instant.now(state.getClock());
    timedOut(stepName, timeoutTimestamp, eventNameCustomizer);
  }

  protected void timedOut(String stepName, Instant timeoutTimestamp, EventNameCustomizer eventNameCustomizer) {
    state.addStep(StepExecution.timedOut(stepName, timeoutTimestamp));
    state.eventAppender().append(timeoutStep(context, stepName, timeoutTimestamp, eventNameCustomizer));
  }

}
