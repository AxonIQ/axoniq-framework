package io.axoniq.workflow.runtime.api.primitives;

import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import jakarta.annotation.Nonnull;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;

/**
 * Default execute command implementation.
 */
public class WorkflowStepResultExecuteCommand implements ExecutePrimitive.ExecuteCommand<WorkflowStepResult> {

  private final String stepName;
  private final Map<String, Object> local;
  private final PayloadProcessor action;
  private final PayloadReducer parameterMapping;
  private final PayloadReducer resultMapping;
  private final Duration timeout;
  private final EventNameCustomizer eventNameCustomizer;

  public WorkflowStepResultExecuteCommand(
    @Nonnull String stepName,
    @Nonnull Map<String, Object> local,
    @Nonnull PayloadProcessor action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {
    this.stepName = stepName;
    this.local = local;
    this.action = action;
    this.parameterMapping = parameterMapping;
    this.resultMapping = resultMapping;
    this.timeout = timeout;
    this.eventNameCustomizer = eventNameCustomizer;
  }

  @Nonnull
  @Override
  public String stepName() {
    return stepName;
  }

  @Nonnull
  @Override
  public Map<String, Object> local() {
    return local;
  }

  @Nonnull
  @Override
  public PayloadProcessor action() {
    return action;
  }

  @Nonnull
  @Override
  public PayloadReducer parameterMapping() {
    return parameterMapping;
  }

  @Nonnull
  @Override
  public PayloadReducer resultMapping() {
    return resultMapping;
  }

  @Nonnull
  @Override
  public Duration timeout() {
    return timeout;
  }

  @Nonnull
  @Override
  public EventNameCustomizer eventNameCustomizer() {
    return eventNameCustomizer;
  }

  @Override
  public WorkflowStepResult result(@Nonnull WorkflowStepResult result) {
    return result;
  }
}
