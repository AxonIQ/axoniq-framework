package io.axoniq.workflow.runtime.engine.streaming;

import io.axoniq.workflow.runtime.engine.WorkflowServices;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

public class EventSubscriptionManager {

  private static final Logger logger = LoggerFactory.getLogger(EventSubscriptionManager.class);
  private final WorkflowServices workflowServices;
  private final Map<EventSubscription, List<CompletableFuture<?>>> subscriptions = new ConcurrentHashMap<>();

  public EventSubscriptionManager(WorkflowServices workflowServices) {
    this.workflowServices = workflowServices;
  }

  public <T> CompletableFuture<T> subscribe(QualifiedName qualifiedName, Predicate<EventMessage> predicate, Class<T> clazz) {
    var subscription = new EventSubscription(qualifiedName, predicate, clazz);
    var future = new CompletableFuture<T>();
    subscriptions.compute(subscription, (key, existingFutures) -> {
      List<CompletableFuture<?>> newFutures = existingFutures != null ? existingFutures : new ArrayList<>();
      newFutures.add(future);
      return newFutures;
    });
    return future;
  }

  public void onEvent(EventMessage event) {
    // must run async, because the step completion event must be received to acknowledge the step.
    CompletableFuture.supplyAsync(() -> {
      var result = new AtomicBoolean(false);
      subscriptions.keySet().forEach(subscription -> {
        boolean eventMatchesCondition = event.type().qualifiedName().equals(subscription.qualifiedName)
          && subscription.predicate().test(event);

        if (eventMatchesCondition) {
          List<CompletableFuture<?>> removedFutures = subscriptions.remove(subscription);
          if (removedFutures != null) {
            logger.debug("Received event {} the workflow was waiting for", event);
            Object payload = event.payloadAs(subscription.payloadClass());

            for (CompletableFuture<?> future : removedFutures) {
              if (future.isCompletedExceptionally() || future.isCancelled()) {
                logger.error("Failed on event {}, waiting future is already completed / cancelled", event);
                // Skip this future and continue with others
              } else {
                ((CompletableFuture) future).complete(payload);
                result.set(true);
              }
            }
          }
        }
      });
      return result.get();
    }, workflowServices.getExecutor());
  }

  record EventSubscription(
    QualifiedName qualifiedName,
    Predicate<EventMessage> predicate,
    Class<?> payloadClass
  ) {
  }
}
