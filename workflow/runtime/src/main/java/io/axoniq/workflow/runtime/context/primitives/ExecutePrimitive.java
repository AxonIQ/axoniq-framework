package io.axoniq.workflow.runtime.context.primitives;

import io.axoniq.workflow.runtime.payload.Payload;
import io.axoniq.workflow.runtime.payload.PayloadFunction;
import io.axoniq.workflow.runtime.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.function.Function;
import java.util.function.Supplier;

import static io.axoniq.workflow.runtime.payload.Payload.empty;

public interface ExecutePrimitive {
  default Payload execute(String stepName, Payload payload, PayloadFunction action) {
    return execute(
      stepName,
      payload,
      action,
      PayloadReducer.all(),
      PayloadReducer.all()
    );
  }

  default <T> T execute(String stepName, Payload payload, Class<T> returnType, Function<Payload, T> action) {
    var stepSpecificName = "__" + stepName;
    return
      execute(
        stepName,
        payload,
        p -> {
          var result = action.apply(payload);
          if (result != null) {
            return new Payload().withValue(stepSpecificName, returnType, result);
          } else {
            return empty();
          }
        },
        PayloadReducer.none(), // no global -> local, just pass nothing
        PayloadReducer.global()  // no local -> global, take global
      ).get(stepSpecificName).getAsTyped();
  }

  default <T> T execute(String stepName, Class<T> returnType, Supplier<T> action) {
    return execute(stepName, empty(), returnType, (p) -> action.get());
  }

  default void execute(String stepName, Runnable action) {
    execute(stepName, Void.class, () -> {
      action.run();
      return null;
    });
  }

  /**
   * Call run primitive.
   * @param stepName name of the step.
   * @param local local variables.
   * @param action action to execute.
   * @param parameterMapping mapping reducer for parameters.
   * @param resultMapping mapping reducer for result.
   * @return payload.
   */
  Payload execute(
    @Nonnull String stepName,
    @Nullable Payload local,
    @Nonnull PayloadFunction action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping
  );

}
