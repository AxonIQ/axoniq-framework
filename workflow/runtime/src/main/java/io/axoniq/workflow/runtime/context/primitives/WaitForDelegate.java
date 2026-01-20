package io.axoniq.workflow.runtime.context.primitives;

import io.axoniq.workflow.runtime.context.StepExecution;
import io.axoniq.workflow.runtime.context.StepFailedException;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepStarted;
import io.axoniq.workflow.runtime.event.StepTimedOut;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class WaitForDelegate extends AbstractPrimitiveDelegate implements WaitForPrimitive {

  public WaitForDelegate(WorkflowContextImpl context) {
    super(context);
  }

  @Override
  public <T> CompletableFuture<T> waitFor(
    @Nonnull String stepName,
    @Nonnull Class<T> eventType,
    @Nonnull Predicate<T> eventCondition,
    @Nonnull Duration timeout) {

    var workflowId = context.getWorkflowId();

    Function<T, T> completionHandler = result -> {
      // TODO: For DCB add a tag, for non-DCB add a technical event (Question 1).
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID), new StepCompleted(stepName, result)));
      context.addStep(stepName, StepExecution.completed(stepName, result));
      return result;
    };

    Consumer<Instant> timeoutOccurredHandler = (timeoutTimestamp) -> {
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepTimedOut.ID),
        new StepTimedOut(stepName, timeoutTimestamp))
      );
      context.addStep(stepName, StepExecution.timedOut(stepName, timeoutTimestamp));
    };

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
            // wait for event
            return eventRetriever(context.getStateManager(), eventType, eventCondition)
              .orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
              .thenApply(completionHandler)
              .exceptionally(ex -> {
                if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
                  var timeoutTimestamp = Instant.now(context.getClock());
                  timeoutOccurredHandler.accept(timeoutTimestamp);
                  throw new CompletionException(ex);
                }
                throw new StepFailedException(ex);
              });
          }
        }
      }
    }

    var started = Instant.now(context.getClock());
    context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepStarted.ID), new StepStarted(stepName)));
    context.addStep(stepName, StepExecution.started(stepName, started)); // FIXME -> timestamp should be additional step attribute instead of misusing payload

    return eventRetriever(context.getStateManager(), eventType, eventCondition)
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
          var events = stateManager.getEventByCriteria(eventType)
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
