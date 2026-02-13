package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.primitives.DelegatingExecuteCommand;
import io.axoniq.workflow.runtime.api.primitives.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.primitives.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.primitives.WorkflowStepResultExecuteCommand;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;

public class BlockingExecuteWithResultCommand<T> extends DelegatingExecuteCommand<T> {

  private final Converter converter;
  private final TypeReference<T> type;

  public static <T> BlockingExecuteWithResultCommand<T> blockingLocal(
    String stepName, Map<String, Object> payload, PayloadProcessor action, Duration timeout, TypeReference<T> type, Converter converter
  ) {
    return new BlockingExecuteWithResultCommand<>(
      PrimitiveCommands.localExecute(stepName, payload, action, timeout),
      converter,
      type
    );
  }

  public BlockingExecuteWithResultCommand(
    WorkflowStepResultExecuteCommand command,
    Converter converter,
    TypeReference<T> type
  ) {
    super(command);
    this.converter = converter;
    this.type = type;
  }

  @Override
  public T result(@NotNull WorkflowStepResult result) {
    if (result.isSuccess() && result.<Map<String, Object>>payload().isPresent()) {
      var resultPayload = result.<Map<String, Object>>payload().get();
      return converter.convert(resultPayload, type.getType());
    } else {
      throw result.error().orElseThrow();
    }
  }
}
