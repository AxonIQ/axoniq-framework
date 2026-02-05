package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import org.axonframework.messaging.core.QualifiedName;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;

public interface OtherWaitForEvent extends WaitForPrimitive {

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    var result = waitFor(stepName, new QualifiedName(eventType), e -> predicate.test(e.payloadAs(eventType)), timeout, eventNameCustomizer);
    return fromResult(result, eventType);
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
    var result = waitFor(stepName, new QualifiedName(eventType), e -> predicate.test(e.payloadAs(eventType)), timeout, eventName());
    return fromResult(result, eventType);
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    var result = waitFor(stepName, new QualifiedName(eventType), (e) -> true, timeout, eventNameCustomizer);
    return fromResult(result, eventType);
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
    var result = waitFor(stepName, new QualifiedName(eventType), (e) -> true, timeout, eventName());
    return fromResult(result, eventType);
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType) {
    return this.waitForEvent(stepName, eventType, Duration.ofSeconds(5)); // TODO default
  }

  default void wait(String stepName, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    var result = waitFor(stepName, new QualifiedName(Void.class), (e) -> false, timeout, eventNameCustomizer);
    if (result.isFailure()) {
      throw result.error().get();
    }
  }

  default void wait(String stepName, Duration timeout) {
    wait(stepName, timeout, eventName());
  }

  default <T> T fromResult(StepExecutionResult result, Class<T> eventType) {
    if (result.isSuccess()) {
      return payloadToTypeConverter(eventType).apply((Map<String, Object>) result.payload().get());
    } else {
      throw result.error().get();
    }
  }
}
