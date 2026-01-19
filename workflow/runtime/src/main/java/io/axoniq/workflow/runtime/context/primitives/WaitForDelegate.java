package io.axoniq.workflow.runtime.context.primitives;

import io.axoniq.workflow.runtime.context.StepExecution;
import io.axoniq.workflow.runtime.context.StepFailedException;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepFailed;
import io.axoniq.workflow.runtime.event.StepStarted;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class WaitForDelegate extends AbstractPrimitiveDelegate implements WaitForPrimitive {

  public WaitForDelegate(WorkflowContextImpl context) {
    super(context);
  }

  @Override
  public <T> T waitFor(String stepName, Class<T> eventType, Duration timeout, Predicate<T> eventCondition, TimeoutMode timeoutMode) {
    var workflowId = context.getWorkflowId();
    Function<T, T> applyEvent = result -> {
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID), new StepCompleted(stepName, result)));
      context.addStep(stepName, StepExecution.completed(stepName, result));
      return result;
    };

    BiConsumer<Instant, Throwable> timeoutFailedHandler = (started, ex) -> {
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID),
        new StepFailed(stepName, "Timed out at:" + started.plus(timeout), ex))
      );
      context.addStep(stepName, StepExecution.failed(stepName, ex));
    };

    BiConsumer<Instant, Throwable> timeoutCompletedHandler = (started, ex) -> {
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID),
        new StepCompleted(stepName, null))
      );
      context.addStep(stepName, StepExecution.completed(stepName, null));
    };

    StepExecution existing = context.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          //noinspection unchecked
          return (T) existing.result();
        }
        case FAILED -> throw new StepFailedException(existing.error());
        case STARTED -> {
          Instant started = (Instant) existing.result();
          Duration remainingTimeout = Duration.between(Instant.now(), started.plus(timeout));
          if (remainingTimeout.isNegative()) {
            // started but timed out
            var ex = new StepFailedException("Timed out at:" + started.plus(timeout), new TimeoutException());
            switch (timeoutMode) {
              case FAILED -> timeoutFailedHandler.accept(started, ex);
              case COMPLETED -> timeoutCompletedHandler.accept(started, ex);
              case EXCEPTION -> throw ex;
            }
          } else {
            // wait for event
            return retriever(eventType, remainingTimeout, eventCondition, applyEvent)
              .join();
          }
        }
      }
    }

    var started = Instant.now();
    context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepStarted.ID), new StepStarted(stepName)));
    context.addStep(stepName, StepExecution.started(stepName, started));

    return retriever(eventType, timeout, eventCondition, applyEvent)
      .exceptionally(ex -> {
        switch (timeoutMode) {
          case FAILED -> timeoutFailedHandler.accept(started, ex);
          case COMPLETED -> timeoutCompletedHandler.accept(started, ex);
        }
        return null;
      })
      .join();
  }

  private <T> CompletableFuture<T> retriever(
    Class<T> eventType,
    Duration remainingTimeout,
    Predicate<T> eventCondition,
    Function<T, T> applyEventHandler
  ) {
    return CompletableFuture
      .supplyAsync(() -> {
        while (true) {
          var events = context.getStateManager().getEventByCriteria(eventType)
            .stream()
            .map(e -> e.payloadAs(eventType))
            .filter(eventCondition)
            .toList();
          if (!events.isEmpty()) {
            return events.getFirst();
          }
          try {
            //noinspection BusyWait
            Thread.sleep(100); // FIXME
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for events", e);
          }
        }
      })
      .orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
      .thenApply(applyEventHandler);
  }
}
