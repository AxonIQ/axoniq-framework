package io.axoniq.workflow.runtime.engine.impl.threadsandfutures;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowEventAppender {

  private final EventSink eventSink;
  private final Map<String, CompletableFuture<?>> waitingForEvents = new ConcurrentHashMap<>();

  public WorkflowEventAppender(@Nonnull EventSink eventSink) {
    this.eventSink = eventSink;
  }

  public CompletableFuture<?> appendEvent(EventMessage eventMessage, ProcessingContext processingContext) {
    var future = new CompletableFuture<>();
    waitingForEvents.put(eventMessage.identifier(), future);
    eventSink.publish(processingContext, eventMessage);
    return future;
  }

  public void onWorkflowEvent(EventMessage eventMessage) {
    if (waitingForEvents.containsKey(eventMessage.identifier())) {
      var future = waitingForEvents.remove(eventMessage.identifier());
      future.complete(null);
    }
  }


}
