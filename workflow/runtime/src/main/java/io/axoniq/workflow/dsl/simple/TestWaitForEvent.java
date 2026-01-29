package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.context.DefaultEventNameCustomizer.Builder.eventName;

public interface TestWaitForEvent extends WaitForPrimitive {

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    return (T)waitFor(stepName, eventType, predicate, timeout, t -> typeToPayloadConverter().apply(t), eventNameCustomizer)
      .join()
      .result()
      .get();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
    return (T)waitFor(stepName, eventType, predicate, timeout, t -> typeToPayloadConverter().apply(t), eventName())
      .join()
      .result()
      .get();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    return (T)waitFor(stepName, eventType, (e) -> true, timeout, t -> typeToPayloadConverter().apply(t), eventNameCustomizer)
      .join()
      .result()
      .get();
  }
  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
    var result = waitFor(stepName, eventType, (e) -> true, timeout, t -> typeToPayloadConverter().apply(t), eventName());
    return (T)result
      .join()
      .result()
      .get();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType) {
    return this.waitForEvent(stepName, eventType, Duration.ofSeconds(5)); // TODO default
  }

  default void wait(String stepName, Duration timeout, EventNameCustomizer eventNameCustomizer) {
      var result = waitFor(stepName, Void.class, (e) -> false, timeout, t -> Map.of(), eventNameCustomizer)
        .join();
      if (result.isTimeout()) {
        return;
      } else if (result.isFailure()) {
        throw (StepFailedException)result.error().get(); // FIXME?
      } else if (result.isCanceled()) {
        return; // FIXME?
      }
  }

  default void wait(String stepName, Duration timeout) {
    wait(stepName, timeout, eventName());
  }

}
