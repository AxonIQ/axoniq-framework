package io.axoniq.workflow.runtime.api.primitives;

import io.axoniq.workflow.runtime.api.workflow.PayloadFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public interface ExecutePrimitive {
  /**
   * Call run primitive.
   *
   * @param stepName         name of the step.
   * @param local            local variables.
   * @param action           action to execute.
   * @param parameterMapping mapping reducer for parameters.
   * @param resultMapping    mapping reducer for result.
   * @return payload.
   */
  CompletableFuture<Map<String, Object>> execute(
    @Nonnull String stepName,
    @Nullable Map<String, Object> local,
    @Nonnull PayloadFunction action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout
  );

}
