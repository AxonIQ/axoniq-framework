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

public interface ExecuteInLocalContext extends ExecutePrimitive {

  default Payload execute(String stepName, Payload payload, Function<Payload, Payload> action, EventNameCustomizer eventNameCustomizer) {
    return payload(execute(stepName, payload.getValues(), p -> action.apply(payload(p)).getValues(), eventNameCustomizer));
  }

  default Payload execute(String stepName, Payload payload, Function<Payload, Payload> action) {
    return payload(execute(stepName, payload.getValues(), p -> action.apply(payload(p)).getValues()));
  }

  default Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action, EventNameCustomizer eventNameCustomizer) {
    return execute(stepName, payload, action, local(), all(), Duration.ofSeconds(5), eventNameCustomizer).join();
  }

  default Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action) {
    return execute(stepName, payload, action, local(), all(), Duration.ofSeconds(5), eventName()).join();
  }

  default <T> T execute(String stepName, Payload payload, Class<T> returnType, Function<Payload, T> action) {
    return execute(stepName, payload.getValues(), returnType, (m) -> action.apply(payload(m)));
  }


  default <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType, Function<Map<String, Object>, T> action, EventNameCustomizer eventNameCustomizer) {
    var stepSpecificName = "__" + stepName;
    //noinspection unchecked
    return (T) execute(
      stepName,
      payload,
      p -> {
        var result = action.apply(payload);
        if (result != null) {
          return Map.of(stepSpecificName, result);
        } else {
          return Map.of();
        }
      },
      local(),
      all(),
      Duration.ofSeconds(5),
      eventNameCustomizer
    ).join().get(stepSpecificName);
  }

  default <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType, Function<Map<String, Object>, T> action) {
    return execute(stepName, payload, returnType, action, eventName());
  }

  default <T> T execute(String stepName, Class<T> returnType, Supplier<T> action) {
    return execute(stepName, Map.of(), returnType, (p) -> action.get());
  }

  default <T> T execute(String stepName, Class<T> returnType, Supplier<T> action, EventNameCustomizer eventNameCustomizer) {
    return execute(stepName, Map.of(), returnType, (p) -> action.get(), eventNameCustomizer);
  }

  default void execute(String stepName, Payload payload, Consumer<Payload> action, EventNameCustomizer eventNameCustomizer) {
    execute(stepName, payload, (p) -> {
        action.accept(p);
        return payload();
      }, eventNameCustomizer);
  }

  default void execute(String stepName, Payload payload, Consumer<Payload> action) {
    execute(stepName, payload, (p) -> {
      action.accept(p);
      return payload();
    });
  }

  default void execute(String stepName, Runnable action, EventNameCustomizer eventNameCustomizer) {
    execute(stepName, Void.class, () -> {
      action.run();
      return null;
    }, eventNameCustomizer);
  }

  default void execute(String stepName, Runnable action) {
    execute(stepName, Void.class, () -> {
      action.run();
      return null;
    });
  }

}
