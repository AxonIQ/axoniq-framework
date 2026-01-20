package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.StepExecution;
import io.axoniq.workflow.runtime.engine.StepFailedException;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.StateManager;
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

import static io.axoniq.workflow.runtime.util.EventMessageUtils.completedStep;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.timeoutStep;

public class WaitForDelegate extends AbstractPrimitiveDelegate implements WaitForPrimitive {

  public WaitForDelegate(WorkflowContext context) {
    super(context);
  }

  @Override
  public Function<Object, Map<String, Object>> getDefaultPayloadProjector() {
    return context.getDefaultPayloadProjector();
  }

  @Override
  public <T> CompletableFuture<T> waitFor(
    @Nonnull String stepName,
    @Nonnull Class<T> eventType,
    @Nonnull Predicate<T> eventCondition,
    @Nonnull Duration timeout,
    @Nonnull Function<T, Map<String, Object>> payloadProjector) {

    Function<T, T> completionHandler = result -> {
      // TODO: For DCB add a tag, for non-DCB add a technical util (Question 1).
      var resultPayload = payloadProjector.apply(result);
      context.getStateManager().append(completedStep(context, stepName, resultPayload));
      context.addStep(stepName, StepExecution.completed(stepName, result));
      return result;
    };

    Consumer<Instant> timeoutOccurredHandler = (timeoutTimestamp) -> {
      context.getStateManager().append(timeoutStep(context, stepName, timeoutTimestamp));
      context.addStep(stepName, StepExecution.timedOut(stepName, timeoutTimestamp));
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
    context.getStateManager().append(EventMessageUtils.startedStep(context, stepName, Map.of())); // FIXME: really?
    context.addStep(stepName, StepExecution.started(stepName, started)); // FIXME -> timestamp should be additional step attribute instead of misusing payload

    return eventRetriever.apply(timeout);
  }

  private static <T> CompletableFuture<T> eventRetriever(
    StateManager stateManager,
    Class<T> eventType,
    Predicate<T> eventCondition
  ) {
    return CompletableFuture
      .supplyAsync(() -> {
        /*
         * FIXME: This is an implementation detail of current PoC running in a unit test single-threaded.
         */
        while (true) {
          var events = stateManager.getEventByPayloadType(eventType)
            .stream()
            .map(e -> e.payloadAs(eventType))
            .filter(eventCondition)
            .toList();
          if (!events.isEmpty()) {
            return events.getFirst();
          }
          try {
            //noinspection BusyWait
            Thread.sleep(100); // TODO polling constant
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for events", e);
          }
        }
      });
  }
}
