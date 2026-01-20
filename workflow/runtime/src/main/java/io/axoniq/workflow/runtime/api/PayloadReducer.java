package io.axoniq.workflow.runtime.api;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public interface PayloadReducer extends BiFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

  static PayloadReducer all() {
    return (global, local) -> {
      var result = new HashMap<>(global);
      result.putAll(local);
      return result;
    };
  }

  static PayloadReducer global() {
    return (global, local) -> global;
  }

  static PayloadReducer local() {
    return (global, local) -> local;
  }

  static PayloadReducer none() {
    return (global, local) -> Map.of();
  }

}
