package io.axoniq.workflow.runtime.context.primitives;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface WaitForPrimitive {

  /**
   * Wait for event.
   *
   * @param stepName  name of the workflow step.
   * @param eventType type of event to wait for.
   * @param timeout   maximum wait duration until timeout.
   * @param predicate instance predicate for the event of given type.
   * @param <T> type of event.
   * @return completable future.
   */
  <T> CompletableFuture<T> waitFor(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout);

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
    return this.waitFor(stepName, eventType, predicate, timeout).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
    return this.waitFor(stepName, eventType, (e) -> true, timeout).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType) {
    return waitForEvent(stepName, eventType, Duration.ofSeconds(5)); // TODO default
  }

  default void wait(String stepName, Duration timeout) {
    try {
      this.waitFor(stepName, Void.class, (e) -> false, timeout).join();
    } catch (Exception e) {
      // nothing to do
    }
  }

}
