package io.axoniq.workflow.runtime.engine.impl.threadsandfutures;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import io.axoniq.workflow.runtime.engine.result.StepExecutionResults;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    Function<Instant, StepExecutionResult> timeoutOccurredHandler = (timeoutTimestamp) -> {
      timedOut(stepName, timeoutTimestamp, eventNameCustomizer);
      return StepExecutionResults.timeout(timeout);
    };

    Function<Duration, StepExecutionResult> waitForEvent = (remainingTimeout) ->
      StepExecutionResults.fromFuture(workflowServices.getEventSubscriptionManager().subscribe(
          workflowContext.getWorkflowId(),
          qualifiedName,
          eventCondition
        )
        .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
        .thenApply(result -> {
          var resultPayload = typeToPayloadConverter().apply(result);
          completed(stepName, resultPayload, eventNameCustomizer);
          return StepExecutionResults.completed(result);
        })
        .exceptionally(ex -> {
            if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
              return timeoutOccurredHandler.apply(Instant.now(workflowServices.getClock()));
            }
            if (ex.getCause() instanceof InterruptedException) {
              return StepExecutionResults.cancelled();
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
          var startedPayload = (Map<?, ?>) existing.result();
          Instant startedAt = (Instant) startedPayload.get("started");
          Duration startedDuration = (Duration) startedPayload.get("duration");
          if (startedDuration != timeout) {
            // FIXME
            // the timeout duration has changed => instance migration?
          }
          Duration remainingTimeout = Duration.between(Instant.now(workflowServices.getClock()), startedAt.plus(timeout));
          if (remainingTimeout.isNegative()) {
            return timeoutOccurredHandler.apply(Instant.now(workflowServices.getClock()));
          } else {
            // wait for event
            return waitForEvent.apply(remainingTimeout);
          }
        }
      }
    }

    var payload = Map.<String, Object>of(
      "started", Instant.now(workflowServices.getClock()),
      "duration", timeout
    );

    started(stepName, payload, eventNameCustomizer);

    // wait for event
    return waitForEvent.apply(timeout);
  }
}
