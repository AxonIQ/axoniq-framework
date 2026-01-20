package io.axoniq.workflow.runtime.api;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public interface WaitForPrimitive {

  /**
   * Wait for util.
   *
   * @param stepName  name of the workflow step.
   * @param eventType type of util to wait for.
   * @param timeout   maximum wait duration until timeout.
   * @param predicate instance predicate for the util of given type.
   * @param <T>       type of util.
   * @return completable future.
   */
  <T> CompletableFuture<T> waitFor(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout, Function<T, Map<String, Object>> payloadProjector);

  /**
   * Retrieves default util to payload projector.
   *
   * @return default projector.
   */
  Function<Object, Map<String, Object>> getDefaultPayloadProjector();

}
