package io.axoniq.workflow.runtime.payload;

import java.util.function.Function;

public interface PayloadFunction extends Function<Payload, Payload> {

  static PayloadFunction identity() {
    return payload -> payload;
  }

  static PayloadFunction no() {
    return payload -> Payload.empty();
  }

}
