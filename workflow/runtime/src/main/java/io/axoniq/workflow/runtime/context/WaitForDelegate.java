package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
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
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return workflowContext.typeToPayloadConverter();
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> payloadType) {
    return workflowContext.payloadToTypeConverter(payloadType);
  }

  @Override
  public StepExecutionResult waitFor(
    @Nonnull String stepName,
    @Nonnull QualifiedName qualifiedName,
    @Nonnull Predicate<EventMessage> eventCondition,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {

    Function<Object, StepExecutionResult> completionHandler = result -> {
      var resultPayload = typeToPayloadConverter().apply(result);
      completed(stepName, resultPayload, eventNameCustomizer);
      return StepExecutionResults.completed(result);
    };


    Consumer<Instant> timeoutOccurredHandler = (timeoutTimestamp) -> {
      timedOut(stepName, timeoutTimestamp, eventNameCustomizer);
    };

    Function<Duration, StepExecutionResult> eventRetriever = (remainingTimeout) ->
      new FutureStepExecutionResult(workflowServices.getEventSubscriptionManager().subscribe(
          qualifiedName,
          eventCondition
        )
        .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
        .thenApply(completionHandler)
        .exceptionally(ex -> {
            if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
              timeoutOccurredHandler.accept(Instant.now(workflowState.getClock()));
              return StepExecutionResults.timeout(timeout);
            }
            return StepExecutionResults.failed(new StepFailedException(ex));
          }
        ));

    var existing = workflowState.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          return StepExecutionResults.completed(existing.result());
        }
        case FAILED -> {
          return StepExecutionResults.failed(new StepFailedException(existing.error()));
        }
        case TIMED_OUT -> {
          return StepExecutionResults.timeout(timeout);
        }
        case STARTED -> {
          Instant started = (Instant) existing.result();
          Duration remainingTimeout = Duration.between(Instant.now(workflowState.getClock()), started.plus(timeout));
          if (remainingTimeout.isNegative()) {
            timeoutOccurredHandler.accept(Instant.now(workflowState.getClock()));
            return StepExecutionResults.timeout(timeout);
          } else {
            // wait for event
            return eventRetriever.apply(remainingTimeout);
          }
        }
      }
    }

    var started = Instant.now(workflowState.getClock());
    var payload = Map.<String, Object>of("started", started, "duration", timeout.toString());

    started(stepName, payload, eventNameCustomizer);

    // wait for event
    return eventRetriever.apply(timeout);
  }
}
