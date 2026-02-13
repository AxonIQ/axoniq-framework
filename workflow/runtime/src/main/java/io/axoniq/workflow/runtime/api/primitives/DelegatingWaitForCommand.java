package io.axoniq.workflow.runtime.api.primitives;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.function.Predicate;

public abstract class DelegatingWaitForCommand<T> implements WaitForPrimitive.WaitForCommand<T> {

  private final WorkflowStepResultWaitForCommand delegate;

  public DelegatingWaitForCommand(WorkflowStepResultWaitForCommand delegate) {
    this.delegate = delegate;
  }

  @NotNull
  @Override
  public String stepName() {
    return delegate.stepName();
  }

  @NotNull
  @Override
  public Duration timeout() {
    return delegate.timeout();
  }

  @Nonnull
  @Override
  public QualifiedName qualifiedName() {
    return delegate.qualifiedName();
  }

  @Nonnull
  @Override
  public Predicate<EventMessage> predicate() {
    return delegate.predicate();
  }

  @NotNull
  @Override
  public EventNameCustomizer eventNameCustomizer() {
    return delegate.eventNameCustomizer();
  }

}
