package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.axoniq.workflow.dsl.simple.Payload.payload;
import static io.axoniq.workflow.runtime.api.primitives.PayloadReducer.all;
import static io.axoniq.workflow.runtime.api.primitives.PayloadReducer.local;
import static io.axoniq.workflow.runtime.context.DefaultEventNameCustomizer.Builder.eventName;

public interface TestExecute extends ExecutePrimitive {

  default Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action, EventNameCustomizer eventNameCustomizer) {
    var result = execute(stepName, payload, action, local(), all(), Duration.ofSeconds(5), eventNameCustomizer);
    if (result.isSuccess()) {
      return result.<Map<String, Object>>payload().get();
    } else {
      throw result.error().get();
    }
  }

  default Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action, Duration timeout) {
    var result = execute(stepName, payload, action, local(), all(), timeout, eventName());
    if (result.isSuccess()) {
      return result.<Map<String, Object>>payload().get();
    } else {
      throw result.error().get();
    }
  }

  default <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType, Function<Map<String, Object>, T> action, EventNameCustomizer eventNameCustomizer) {
    var stepSpecificName = "__" + stepName;
    var result = execute(
      stepName,
      payload,
      p -> {
        var stepResult = action.apply(payload);
        if (stepResult != null) {
          return Map.of(stepSpecificName, stepResult);
        } else {
          return Map.of();
        }
      },
      local(),
      all(),
      Duration.ofSeconds(5),
      eventNameCustomizer
    );
    if (result.isSuccess()) {
      //noinspection unchecked
      return (T) result.<Map<String, Object>>payload().get().get(stepSpecificName);
    } else {
      throw result.error().get();
    }
  }

  // simple overloads

  default Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action) {
    return this.execute(stepName, payload, action, Duration.ofSeconds(5));
  }

  default Payload execute(String stepName, Payload payload, Function<Payload, Payload> action, EventNameCustomizer eventNameCustomizer) {
    return payload(this.execute(stepName, payload.getValues(), p -> action.apply(payload(p)).getValues(), eventNameCustomizer));
  }

  default Payload execute(String stepName, Payload payload, Function<Payload, Payload> action) {
    return payload(this.execute(stepName, payload.getValues(), p -> action.apply(payload(p)).getValues()));
  }

  default <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType, Function<Map<String, Object>, T> action) {
    return this.execute(stepName, payload, returnType, action, eventName());
  }

  default <T> T execute(String stepName, Class<T> returnType, Supplier<T> action) {
    return this.execute(stepName, Map.of(), returnType, (p) -> action.get());
  }

  default <T> T execute(String stepName, Class<T> returnType, Supplier<T> action, EventNameCustomizer eventNameCustomizer) {
    return this.execute(stepName, Map.of(), returnType, (p) -> action.get(), eventNameCustomizer);
  }

  default void execute(String stepName, Payload payload, Consumer<Payload> action, EventNameCustomizer eventNameCustomizer) {
    execute(stepName, payload, (p) -> {
      action.accept(p);
      return payload();
    }, eventNameCustomizer);
  }

  default void execute(String stepName, Payload payload, Consumer<Payload> action) {
    this.execute(stepName, payload, (p) -> {
      action.accept(p);
      return payload();
    });
  }

  default void execute(String stepName, Runnable action, EventNameCustomizer eventNameCustomizer) {
    this.execute(stepName, Void.class, () -> {
      action.run();
      return null;
    }, eventNameCustomizer);
  }

  default void execute(String stepName, Runnable action) {
    this.execute(stepName, Void.class, () -> {
      action.run();
      return null;
    });
  }

  default <T> T execute(String stepName, Payload payload, Class<T> returnType, Function<Payload, T> action) {
    return this.execute(stepName, payload.getValues(), returnType, (m) -> action.apply(payload(m)));
  }
}
