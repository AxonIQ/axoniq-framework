package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepStatus;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.util.Utils.sleep;

public class EventMessageUtils {

  public static Predicate<EventMessage> workflowIdFilter(String workflowId) {
    return m -> MetadataUtils.workflowIdFilter(workflowId).test(m.metadata());
  }

  public static EventMessage failedWorkflow(WorkflowContext context, Exception exception) {
    var name = context.getWorkflowId() + "Failed" + "#0.1";
    return new GenericEventMessage(MessageType.fromString(name), exception,
      MetadataUtils.create(context.getWorkflowId())
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
    return CompletableFuture
      .supplyAsync(() -> {
        /*
         * FIXME: This is an implementation detail of current PoC running in a unit test single-threaded.
         */
        while (true) {
          var events = stateManager.getEventByPayloadType(eventType)
            .stream()
            .filter(eventCondition)
            .toList();
          if (!events.isEmpty()) {
            return events.getFirst();
          }
          sleep(100);
        }
      });
  }


  private EventMessageUtils() {
    // avoid
  }
}
