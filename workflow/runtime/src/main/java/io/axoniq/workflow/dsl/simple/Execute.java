package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.workflow.PayloadFunction;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Execute extends ExecutePrimitive {
  default Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadFunction action) {
    return execute(
      stepName,
      payload,
      action,
      PayloadReducer.local(),
      PayloadReducer.all()
    );
  }

  default <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType, Function<Map<String, Object>, T> action) {
    var stepSpecificName = "__" + stepName;
    //noinspection unchecked
    return (T)execute(
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
      PayloadReducer.local(),
      PayloadReducer.all()
    ).get(stepSpecificName);
  }

  default <T> T execute(String stepName, Class<T> returnType, Supplier<T> action) {
    return execute(stepName, Map.of(), returnType, (p) -> action.get());
  }

  default void execute(String stepName, Runnable action) {
    execute(stepName, Void.class, () -> {
      action.run();
      return null;
    });
  }

  default Map<String, Object> execute(
    String stepName,
    Map<String, Object> local,
    PayloadFunction action,
    PayloadReducer parameterMapping,
    PayloadReducer resultMapping) {
    return execute(stepName, local, action, parameterMapping, resultMapping, Duration.ofSeconds(5)).join();
  }

}
