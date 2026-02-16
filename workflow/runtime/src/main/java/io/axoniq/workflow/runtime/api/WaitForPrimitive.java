package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

public interface WaitForPrimitive {

  /**
   * Wait for event.
   *
   * @param stepName            name of the workflow step.
   * @param qualifiedName       type of event to wait for.
   * @param timeout             maximum wait duration until timeout.
   * @param predicate           execution predicate for the event of given type.
   * @param eventNameCustomizer event name customizer.
   * @return result.
   */
  @Nonnull
  WorkflowStepResult waitFor(
    @Nonnull String stepName,
    @Nonnull QualifiedName qualifiedName,
    @Nonnull Predicate<EventMessage> predicate,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  );

  default <T> T waitFor(@Nonnull WaitForCommand<T> command) {
    Objects.requireNonNull(command, "command must not be null");
    return command.result(
      waitFor(
        command.stepName(),
        command.qualifiedName(),
        command.predicate(),
        command.timeout(),
        command.eventNameCustomizer()
      )
    );
  }

  interface WaitForCommand<T> {
    @Nonnull
    String stepName();

    @Nonnull
    QualifiedName qualifiedName();

    @Nonnull
    Predicate<EventMessage> predicate();

    @Nonnull
    Duration timeout();

    @Nonnull
    EventNameCustomizer eventNameCustomizer();

    T result(@Nonnull WorkflowStepResult result);
  }

}
