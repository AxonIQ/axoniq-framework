package io.axoniq.workflow.runtime.api.primitives;

import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;


public interface ExecutePrimitive {
  /**
   * Execute primitive.
   *
   * @param stepName            name of the step.
   * @param local               local context passed to the call.
   * @param action              action to execute.
   * @param parameterMapping    reducer for parameters (to reduce local and workflow contexts -> effective parameters).
   * @param resultMapping       reducer for result (to reduce result and global context -> global context after action).
   * @param timeout             timeout of the action.
   * @param eventNameCustomizer event name customizer.
   * @return result.
   */
  @Nonnull
  WorkflowStepResult execute(
    @Nonnull String stepName,
    @Nullable Map<String, Object> local,
    @Nonnull PayloadProcessor action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  );

  @Nonnull
  default <T> T execute(@Nonnull ExecuteCommand<T> command) {
    Objects.requireNonNull(command, "Command is required");
    return
      command.result(
        execute(command.stepName(), command.local(), command.action(), command.parameterMapping(), command.resultMapping(), command.timeout(), command.eventNameCustomizer()));
  }

  interface ExecuteCommand<T> {
    @Nonnull
    String stepName();

    @Nonnull
    Map<String, Object> local();

    @Nonnull
    PayloadProcessor action();

    @Nonnull
    PayloadReducer parameterMapping();

    @Nonnull
    PayloadReducer resultMapping();

    @Nonnull
    Duration timeout();

    @Nonnull
    EventNameCustomizer eventNameCustomizer();

    T result(@Nonnull WorkflowStepResult result);
  }

}
