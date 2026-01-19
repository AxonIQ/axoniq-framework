package io.axoniq.workflow.runtime.context.primitives;

import io.axoniq.workflow.runtime.context.StepExecution;
import io.axoniq.workflow.runtime.context.StepFailedException;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepFailed;
import io.axoniq.workflow.runtime.event.StepStarted;
import io.axoniq.workflow.runtime.payload.Payload;
import io.axoniq.workflow.runtime.payload.PayloadFunction;
import io.axoniq.workflow.runtime.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;

public class ExecuteDelegate extends AbstractPrimitiveDelegate implements ExecutePrimitive {

  public ExecuteDelegate(WorkflowContextImpl context) {
    super(context);
  }

  @Override
  public Payload execute(
    @Nonnull String stepName,
    @Nullable Payload local,
    @Nonnull PayloadFunction action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping)
  {
    StepExecution existing = context.steps.get(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          var result = (Payload) existing.result();
          context.modifyPayload(p -> resultMapping.apply(p, result)); // reduce results back
          return result;
        }
        case FAILED -> throw new StepFailedException(existing.error());
        case IN_PROGRESS -> {
          // Fall through to re-run the step
        }
      }
    }

    var workflowId = context.getWorkflowId();
    context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepStarted.ID), new StepStarted(stepName)));
    context.steps.put(stepName, StepExecution.inProgress(stepName, Instant.now()));

    try {
      var parameters = parameterMapping.apply(context.getPayload(), local); // local copy of the payload

      // step execution
      Payload result = action.apply(parameters);
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID), new StepCompleted(stepName, result)));
      context.steps.put(stepName, StepExecution.completed(stepName, result));

      context.modifyPayload(p -> resultMapping.apply(p, result)); // write back payload
      return result;
    } catch (Throwable e) {
      context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepFailed.ID), new StepFailed(stepName, e.getMessage(), e)));
      context.steps.put(stepName, StepExecution.failed(stepName, e));
      throw e;
    }
  }
}
