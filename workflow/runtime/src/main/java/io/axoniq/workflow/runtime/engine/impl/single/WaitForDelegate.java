package io.axoniq.workflow.runtime.engine.impl.single;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class WaitForDelegate extends AbstractStepExecutor implements WaitForPrimitive {

  public WaitForDelegate(
    @Nonnull WorkflowContext workflowContext,
    @Nonnull WorkflowState workflowState,
    @Nonnull WorkflowServices workflowServices
  ) {
    super(workflowContext, workflowState, workflowServices);
  }

  @Override
  public StepExecutionResult waitFor(
    @NotNull String stepName,
    @NotNull QualifiedName qualifiedName,
    @NotNull Predicate<EventMessage> predicate,
    @NotNull Duration timeout,
    @NotNull EventNameCustomizer eventNameCustomizer
  ) {
    return null;
  }

  @Override
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return workflowContext.typeToPayloadConverter();
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> payloadType) {
    return workflowContext.payloadToTypeConverter(payloadType);
  }
}
