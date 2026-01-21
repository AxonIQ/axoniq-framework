package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.StepExecution;
import io.axoniq.workflow.runtime.engine.StepFailedException;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
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

import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;

public class WaitForDelegate extends AbstractContextAwarePrimitiveDelegate implements WaitForPrimitive {

  public WaitForDelegate(WorkflowContext context) {
    super(context);
  }

  @Override
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return context.typeToPayloadConverter();
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(Class<T> payloadType) {
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
      context.getStateManager().append(completedStep(context, stepName, resultPayload, eventNameCustomizer)); // TODO: For DCB add a tag, for non-DCB add a technical util (Question 1).
      context.addStep(StepExecution.completed(stepName, result));
      return result;
    };

    Consumer<Instant> timeoutOccurredHandler = (timeoutTimestamp) -> {
      context.getStateManager().append(timeoutStep(context, stepName, timeoutTimestamp, eventNameCustomizer));
      context.addStep(StepExecution.timedOut(stepName, timeoutTimestamp));
    };

    Function<Duration, CompletableFuture<T>> eventRetriever = (remainingTimeout) ->
      eventRetriever(context.getStateManager(), eventType, eventCondition)
        .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
        .thenApply(completionHandler)
        .exceptionally(ex -> {
          if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
            var timeoutTimestamp = Instant.now(context.getClock());
            timeoutOccurredHandler.accept(timeoutTimestamp);
            throw new CompletionException(ex);
          }
          throw new StepFailedException(ex);
        });

    StepExecution existing = context.getStep(stepName);
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
          Duration remainingTimeout = Duration.between(Instant.now(context.getClock()), started.plus(timeout));
          if (remainingTimeout.isNegative()) {
            timeoutOccurredHandler.accept(Instant.now(context.getClock()));
            return CompletableFuture.failedFuture(new TimeoutException("Timed out waiting for " + stepName));
          } else {
            // wait for util
            return eventRetriever.apply(remainingTimeout);
          }
        }
      }
    }

    var started = Instant.now(context.getClock());
    var payload = Map.<String, Object>of("started", started, "duration", timeout.toString());
    context.getStateManager().append(EventMessageUtils.startedStep(context, stepName, payload, eventNameCustomizer));
    context.addStep(StepExecution.started(stepName, started)); // FIXME -> timestamp should be additional step attribute instead of misusing payload

    return eventRetriever.apply(timeout);
  }
}
