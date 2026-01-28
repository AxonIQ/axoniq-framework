package io.axoniq.workflow.runtime.___stash;

import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class SubscriptionUtil {
  public static <T> CompletableFuture<T> eventRetriever(
    StateManager stateManager,
    Class<T> eventType,
    Predicate<T> eventCondition
  ) {
    return eventMessageRetriever(
      stateManager,
      eventType,
      (m) -> eventCondition.test(m.payloadAs(eventType))
    ).thenApply(e -> e.payloadAs(eventType));
  }

  public static <T> CompletableFuture<EventMessage> eventMessageRetriever(
    StateManager stateManager,
    Class<T> eventType,
    Predicate<EventMessage> eventCondition
  ) {
    CompletableFuture<EventMessage> future = new CompletableFuture<>();

    // Check if event already exists
    var existing = stateManager.getEventByPayloadType(eventType)
      .stream()
      .filter(eventCondition)
      .findFirst();

    if (existing.isPresent()) {
      return CompletableFuture.completedFuture(existing.get());
    }

    // Subscribe for future events
    var subscription = stateManager.subscribe(new QualifiedName(eventType), eventCondition, event -> {
      future.complete(event);
      return true; // Remove streaming after completion
    });

    // Cancel streaming if the future is cancelled externally
    future.whenComplete((result, ex) -> {
      if (ex != null || future.isCancelled()) {
        subscription.cancel();
      }
    });

    return future;
  }

}
