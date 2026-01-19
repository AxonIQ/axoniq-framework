package io.axoniq.workflow.runtime.context.primitives;

import io.axoniq.workflow.runtime.context.StepExecution;
import io.axoniq.workflow.runtime.context.StepFailedException;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepStarted;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

public class WaitForDelegate extends AbstractPrimitiveDelegate implements WaitForPrimitive {

  public WaitForDelegate(WorkflowContextImpl context) {
    super(context);
  }

  @Override
  public <T> T waitFor(String stepName, Class<T> eventType, Duration timeout, Predicate<T> eventCondition) {
    var workflowId = context.getWorkflowId();
    Function<T, T> applyEvent = result -> {
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID), new StepCompleted(stepName, result)));
      context.steps.put(stepName, StepExecution.completed(stepName, result));
      return result;
    };
    StepExecution existing = context.steps.get(stepName);
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
            throw new StepFailedException("Timed out at:" + started.plus(timeout), new TimeoutException());
          } else {
            // wait for event
            return createRetrievalFuture(eventType, remainingTimeout, eventCondition)
              .thenApply(applyEvent)
              .join();
          }
        }
      }
    }

    context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepStarted.ID), new StepStarted(stepName)));
    context.steps.put(stepName, StepExecution.started(stepName, Instant.now()));

    return createRetrievalFuture(eventType, timeout, eventCondition)
      .thenApply(applyEvent)
      .join();
  }

  private <T> CompletableFuture<T> createRetrievalFuture(Class<T> eventType, Duration remainingTimeout, Predicate<T> eventCondition) {
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
      .orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
