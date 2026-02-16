package io.axoniq.workflow.runtime.api;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

@FunctionalInterface
public interface PayloadReducer extends BiFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

  static PayloadReducer all() {
    return (context, local) -> {
      var result = new HashMap<>(context);
      result.putAll(local);
      return result;
    };
  }

  static PayloadReducer context() {
    return (context, local) -> context;
  }

  static PayloadReducer local() {
    return (context, local) -> local;
  }

  static PayloadReducer none() {
    return (global, local) -> Map.of();
  }

}
