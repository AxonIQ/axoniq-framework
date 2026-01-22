package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepStatus;
import io.axoniq.workflow.runtime.engine.WorkflowStatus;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class EventMessageUtils {

  public static Predicate<EventMessage> workflowIdFilter(String workflowId) {
    return m -> MetadataUtils.workflowIdFilter(workflowId).test(m.metadata());
  }

  public static EventMessage failedWorkflow(WorkflowContext context, Exception exception) {
    var name = context.getWorkflowId() + "Failed" + "#0.1";
    var payload = new HashMap<String, Object>();
    payload.put("message", exception.getMessage());
    if (exception.getCause() != null) {
      payload.put("cause", exception.getCause().toString());
    }
    return new GenericEventMessage(MessageType.fromString(name), payload,
      MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.FAILED)
    );
  }

  public static EventMessage completedWorkflow(WorkflowContext context) {
    var name = context.getWorkflowId() + "Completed" + "#0.1";
    return new GenericEventMessage(MessageType.fromString(name), Map.of(),
      MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.COMPLETED)
    );
  }

  public static EventMessage startedStep(WorkflowContext context, String stepName, Map<String, Object> local, EventNameCustomizer customizer) {
    var name = customizer.getEventName(stepName, local, StepStatus.STARTED);
    return new GenericEventMessage(MessageType.fromString(name), local,
      MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.STARTED)
    );
  }

  public static EventMessage completedStep(WorkflowContext context, String stepName, Map<String, Object> result, EventNameCustomizer customizer) {
    var name = customizer.getEventName(stepName, result, StepStatus.COMPLETED);
    return new GenericEventMessage(MessageType.fromString(name), result,
      MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.COMPLETED)
    );
  }

  public static EventMessage failStep(WorkflowContext context, String stepName, Throwable exception, EventNameCustomizer customizer) {
    var name = customizer.getEventName(stepName, Map.of(), StepStatus.COMPLETED);
    return new GenericEventMessage(MessageType.fromString(name), exception,
      MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.FAILED)
    );
  }

  public static EventMessage timeoutStep(WorkflowContext context, String stepName, Instant time, EventNameCustomizer customizer) {
    var name = customizer.getEventName(stepName, Map.of(), StepStatus.COMPLETED);
    return new GenericEventMessage(MessageType.fromString(name), time,
      MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.TIMED_OUT)
    );
  }

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
      return true; // Remove subscription after completion
    });

    // Cancel subscription if the future is cancelled externally
    future.whenComplete((result, ex) -> {
      if (ex != null || future.isCancelled()) {
        subscription.cancel();
      }
    });

    return future;
  }


  private EventMessageUtils() {
    // avoid
  }
}
