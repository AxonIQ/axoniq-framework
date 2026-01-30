package io.axoniq.workflow.runtime.api.primitives;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.function.Predicate;

public interface WaitForPrimitive extends ConverterAware {

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
  StepExecutionResult waitFor(
    @Nonnull String stepName,
    @Nonnull QualifiedName qualifiedName,
    @Nonnull Predicate<EventMessage> predicate,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  );

}
