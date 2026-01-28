package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;

public class EventMessageUtils {

  public static Predicate<EventMessage> workflowIdFilter(String workflowId) {
    return m -> MetadataUtils.workflowIdFilter(workflowId).test(m.metadata());
  }

  public static EventMessage startedWorkflow(WorkflowContext context, EventNameCustomizer customizer) {
    var name = customizer.getEventName(context.getWorkflowId(), context.getPayload(), WorkflowStatus.STARTED);
    return new GenericEventMessage(MessageType.fromString(name), Map.of(),
      MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.STARTED)
    );
  }
  public static EventMessage completedWorkflow(WorkflowContext context, EventNameCustomizer customizer) {
    var name = customizer.getEventName(context.getWorkflowId(), context.getPayload(), WorkflowStatus.COMPLETED);
    return new GenericEventMessage(MessageType.fromString(name), Map.of(),
      MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.COMPLETED)
    );
  }

  public static EventMessage failedWorkflow(WorkflowContext context, Exception exception, EventNameCustomizer customizer) {
    var name = customizer.getEventName(context.getWorkflowId(), context.getPayload(), WorkflowStatus.FAILED);
    return new GenericEventMessage(MessageType.fromString(name), exception,
      MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.FAILED)
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

  private EventMessageUtils() {
    // avoid
  }
}
