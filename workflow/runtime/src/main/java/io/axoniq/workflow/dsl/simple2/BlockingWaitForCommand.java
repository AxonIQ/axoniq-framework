package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.primitives.DelegatingWaitForCommand;
import io.axoniq.workflow.runtime.api.primitives.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.primitives.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.primitives.WorkflowStepResultWaitForCommand;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

public class BlockingWaitForCommand<T> extends DelegatingWaitForCommand<T> {

  private final Converter converter;
  private final TypeReference<T> type;

  public static <T> BlockingWaitForCommand<T> blocking(
    String stepName, QualifiedName qualifiedName, Predicate<EventMessage> predicate, Duration timeout, TypeReference<T> type, Converter converter
  ) {
    return new BlockingWaitForCommand<>(
      PrimitiveCommands.waitFor(stepName, qualifiedName, predicate, timeout),
      converter,
      type
    );
  }

  public BlockingWaitForCommand(
    WorkflowStepResultWaitForCommand command,
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
