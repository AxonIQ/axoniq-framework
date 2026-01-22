package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import io.axoniq.workflow.runtime.engine.StepExecution;
import io.axoniq.workflow.runtime.engine.StepFailedException;
import jakarta.annotation.Nonnull;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.eventRetriever;

public class WaitForDelegate extends AbstractPrimitiveDelegate implements WaitForPrimitive {

  public WaitForDelegate(WorkflowContext context, WorkflowState workflowState) {
    super(context, workflowState);
  }

  @Override
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return context.typeToPayloadConverter();
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> payloadType) {
    return context.payloadToTypeConverter(payloadType);
  }

  @Override
  public <T> CompletableFuture<T> waitFor(
    @Nonnull String stepName,
    @Nonnull Class<T> eventType,
    @Nonnull Predicate<T> eventCondition,
    @Nonnull Duration timeout,
    @Nonnull Function<T, Map<String, Object>> converter,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {

    Function<T, T> completionHandler = result -> {
      var resultPayload = converter.apply(result);
      completed(stepName, resultPayload, eventNameCustomizer);
      return result;
    };

    Consumer<Instant> timeoutOccurredHandler = (timeoutTimestamp) -> {
      timedOut(stepName, timeoutTimestamp, eventNameCustomizer);
    };

    Function<Duration, CompletableFuture<T>> eventRetriever = (remainingTimeout) ->
      eventRetriever(state.stateManager(), eventType, eventCondition)
        .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
        .thenApply(completionHandler)
        .exceptionally(ex -> {
          if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
            var timeoutTimestamp = Instant.now(state.getClock());
            timeoutOccurredHandler.accept(timeoutTimestamp);
            throw new CompletionException(ex);
          }
          throw new StepFailedException(ex);
        });

    StepExecution existing = state.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          //noinspection unchecked
          return CompletableFuture.completedFuture((T) existing.result());
        }
        case FAILED -> {
          return CompletableFuture.failedFuture(new StepFailedException(existing.error()));
        }
        case TIMED_OUT -> {
          return CompletableFuture.failedFuture(new TimeoutException("Timed out waiting for " + stepName));
        }
        case STARTED -> {
          Instant started = (Instant) existing.result();
          Duration remainingTimeout = Duration.between(Instant.now(state.getClock()), started.plus(timeout));
          if (remainingTimeout.isNegative()) {
            timeoutOccurredHandler.accept(Instant.now(state.getClock()));
            return CompletableFuture.failedFuture(new TimeoutException("Timed out waiting for " + stepName));
          } else {
            // wait for util
            return eventRetriever.apply(remainingTimeout);
          }
        }
      }
    }

    var started = Instant.now(state.getClock());
    var payload = Map.<String, Object>of("started", started, "duration", timeout.toString());
    started(stepName, payload, eventNameCustomizer);

    return eventRetriever.apply(timeout);
  }
}
