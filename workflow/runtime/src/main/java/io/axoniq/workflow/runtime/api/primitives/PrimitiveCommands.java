package io.axoniq.workflow.runtime.api.primitives;

import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;

public class PrimitiveCommands {

  public static WorkflowStepResultExecuteCommand localExecute(
    @Nonnull String stepName,
    @Nonnull Map<String, Object> local,
    @Nonnull PayloadProcessor action,
    @Nonnull Duration timeout
  ) {
    return new WorkflowStepResultExecuteCommand(stepName, local, action, PayloadReducer.local(), PayloadReducer.all(), timeout, eventName());
  }

  public static WorkflowStepResultWaitForCommand waitFor(
    @Nonnull String stepName,
    @Nonnull QualifiedName qualifiedName,
    @Nonnull Predicate<EventMessage> predicate,
    @Nonnull Duration timeout
  ) {
    return new WorkflowStepResultWaitForCommand(stepName, qualifiedName, predicate, timeout, eventName());
  }

  public static WorkflowStepResultWaitForCommand wait(
    @Nonnull String stepName,
    @Nonnull Duration timeout
  ) {
    return new WorkflowStepResultWaitForCommand(stepName, new QualifiedName(Void.class), e -> false, timeout, eventName());
  }


  private PrimitiveCommands() {

  }
}
