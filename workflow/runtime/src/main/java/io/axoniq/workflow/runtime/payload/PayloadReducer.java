package io.axoniq.workflow.runtime.payload;

import java.util.function.BiFunction;

public interface PayloadReducer extends BiFunction<Payload, Payload, Payload> {

  static PayloadReducer all() {
    return (global, local) -> Payload.of(global).plus(local);
  }

  static PayloadReducer global() {
    return (global, local) -> global;
  }

  static PayloadReducer local() {
    return (global, local) -> local;
  }

  static PayloadReducer none() {
    return (global, local) -> Payload.empty();
  }

}
