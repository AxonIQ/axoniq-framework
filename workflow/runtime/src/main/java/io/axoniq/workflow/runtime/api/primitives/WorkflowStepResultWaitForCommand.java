package io.axoniq.workflow.runtime.api.primitives;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.function.Predicate;

public class WorkflowStepResultWaitForCommand implements WaitForPrimitive.WaitForCommand<WorkflowStepResult> {

  private final String stepName;
  private final Duration timeout;
  private final EventNameCustomizer eventNameCustomizer;
  private final QualifiedName qualifiedName;
  private final Predicate<EventMessage> predicate;

  public WorkflowStepResultWaitForCommand(
    @Nonnull String stepName,
    @Nonnull QualifiedName qualifiedName,
    @Nonnull Predicate<EventMessage> predicate,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {
    this.stepName = stepName;
    this.timeout = timeout;
    this.qualifiedName = qualifiedName;
    this.predicate = predicate;
    this.eventNameCustomizer = eventNameCustomizer;
  }

  @Nonnull
  @Override
  public String stepName() {
    return stepName;
  }

  @Nonnull
  @Override
  public QualifiedName qualifiedName() {
    return qualifiedName;
  }

  @Nonnull
  @Override
  public Predicate<EventMessage> predicate() {
    return predicate;
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
