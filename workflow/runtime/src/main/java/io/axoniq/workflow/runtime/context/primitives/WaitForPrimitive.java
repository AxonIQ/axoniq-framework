package io.axoniq.workflow.runtime.context.primitives;

import java.time.Duration;
import java.util.function.Predicate;

public interface WaitForPrimitive {

  <T> T waitFor(String stepName, Class<T> eventType, Duration timeout, Predicate<T> predicate, TimeoutMode timoutMode);

  default <T> T waitFor(String stepName, Class<T> eventType, Duration timeout, Predicate<T> predicate) {
    return waitFor(stepName, eventType, timeout, predicate, TimeoutMode.EXCEPTION);
  }

  default <T> T waitFor(String stepName, Class<T> eventType, Duration timeout) {
    return waitFor(stepName, eventType, timeout, (e) -> true, TimeoutMode.EXCEPTION);
  }

  default <T> T waitFor(String stepName, Class<T> eventType) {
    return waitFor(stepName, eventType, Duration.ofSeconds(5)); // default
  }

  default void waitFor(String stepName, Duration timeout) {
    try {
      waitFor(stepName, Void.class, timeout, (e) -> false, TimeoutMode.COMPLETED);
    } catch (Exception e) {
      // nothing to do
    }
  }

  enum TimeoutMode {
    EXCEPTION,
    FAILED,
    COMPLETED
  }
}
