package io.axoniq.workflow.runtime.context.primitives;

import java.time.Duration;
import java.util.function.Predicate;

public interface WaitForPrimitive {

  <T> T waitFor(String stepName, Class<T> eventType, Duration timeout, Predicate<T> predicate);

  default <T> T waitFor(String stepName, Class<T> eventType, Duration timeout) {
    return waitFor(stepName, eventType, timeout, (e) -> true);
  }

  default <T> T waitFor(String stepName, Class<T> eventType) {
    return waitFor(stepName, eventType, Duration.ofSeconds(5)); // default
  }
}
