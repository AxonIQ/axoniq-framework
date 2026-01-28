package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.context.DefaultEventNameCustomizer.Builder.eventName;

public interface TestWaitForEvent extends WaitForPrimitive {

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    return this.waitFor(stepName, eventType, predicate, timeout, t -> typeToPayloadConverter().apply(t), eventNameCustomizer).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
    return this.waitFor(stepName, eventType, predicate, timeout, t -> typeToPayloadConverter().apply(t), eventName()).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    return this.waitFor(stepName, eventType, (e) -> true, timeout, t -> typeToPayloadConverter().apply(t), eventNameCustomizer).join();
  }
  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
    var future = this.waitFor(stepName, eventType, (e) -> true, timeout, t -> typeToPayloadConverter().apply(t), eventName());
    return future.join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType) {
    return waitForEvent(stepName, eventType, Duration.ofSeconds(5)); // TODO default
  }

  default void wait(String stepName, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    try {
      this.waitFor(stepName, Void.class, (e) -> false, timeout, t -> Map.of(), eventNameCustomizer).join();
    } catch (Exception e) {
      // nothing to do
    }
  }

  default void wait(String stepName, Duration timeout) {
    wait(stepName, timeout, eventName());
  }

}
