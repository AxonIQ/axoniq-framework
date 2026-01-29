package io.axoniq.workflow.runtime.api.primitives;

import jakarta.annotation.Nonnull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public interface WaitForPrimitive extends ConverterAware {

  /**
   * Wait for event.
   *
   * @param stepName  name of the workflow step.
   * @param eventType type of event to wait for.
   * @param timeout   maximum wait duration until timeout.
   * @param predicate execution predicate for the event of given type.
   * @param <T>       type of event.
   * @param converter converter from event to payload.
   * @param eventNameCustomizer event name customizer.
   * @return completable future.
   */
  <T> CompletableFuture<StepResult> waitFor(
    @Nonnull String stepName,
    @Nonnull Class<T> eventType,
    @Nonnull Predicate<T> predicate,
    @Nonnull Duration timeout,
    @Nonnull Function<T, Map<String, Object>> converter,
    @Nonnull EventNameCustomizer eventNameCustomizer
  );

}
