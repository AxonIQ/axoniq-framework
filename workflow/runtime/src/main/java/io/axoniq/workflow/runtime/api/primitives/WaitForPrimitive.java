package io.axoniq.workflow.runtime.api.primitives;

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
   * @param predicate instance predicate for the event of given type.
   * @param <T>       type of event.
   * @param converter converter from event to payload.
   * @return completable future.
   */
  <T> CompletableFuture<T> waitFor(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout, Function<T, Map<String, Object>> converter);

}
