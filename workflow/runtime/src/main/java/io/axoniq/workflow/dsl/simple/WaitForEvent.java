package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.WaitForPrimitive;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

public interface WaitForEvent extends WaitForPrimitive {

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
    return this.waitFor(stepName, eventType, predicate, timeout,t -> getDefaultPayloadProjector().apply(t)).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
    return this.waitFor(stepName, eventType, (e) -> true, timeout, t -> getDefaultPayloadProjector().apply(t)).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType) {
    return waitForEvent(stepName, eventType, Duration.ofSeconds(5)); // TODO default
  }

  default void wait(String stepName, Duration timeout) {
    try {
      this.waitFor(stepName, Void.class, (e) -> false, timeout, t -> Map.of()).join();
    } catch (Exception e) {
      // nothing to do
    }
  }
}
