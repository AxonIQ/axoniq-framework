package io.axoniq.workflow.runtime.context.primitives;

import io.axoniq.workflow.runtime.context.StepExecution;
import io.axoniq.workflow.runtime.context.StepFailedException;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepFailed;
import io.axoniq.workflow.runtime.event.StepStarted;
import io.axoniq.workflow.runtime.event.StepTimedOut;
import io.axoniq.workflow.runtime.payload.Payload;
import io.axoniq.workflow.runtime.payload.PayloadFunction;
import io.axoniq.workflow.runtime.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class ExecuteDelegate extends AbstractPrimitiveDelegate implements ExecutePrimitive {

  public ExecuteDelegate(WorkflowContextImpl context) {
    super(context);
  }

  @Override
  public CompletableFuture<Payload> execute(
    @Nonnull String stepName,
    @Nullable Payload local,
    @Nonnull PayloadFunction action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout
  ) {
    StepExecution existing = context.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          var result = (Payload) existing.result();
          context.modifyPayload(p -> resultMapping.apply(p, result)); // reduce results back
          return CompletableFuture.completedFuture(result);
        }
        case FAILED -> {
          return CompletableFuture.failedFuture(new StepFailedException(existing.error()));
        }
        case TIMED_OUT -> {
          return CompletableFuture.failedFuture(new TimeoutException("Timed out waiting for " + stepName));
        }
      }
    }

    var workflowId = context.getWorkflowId();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepStarted.ID), new StepStarted(stepName)));
            context.addStep(stepName, StepExecution.started(stepName, Instant.now(context.getClock())));
            var parameters = parameterMapping.apply(context.getPayload(), local); // local copy of the payload

            // step execution
            Payload result = action.apply(parameters);
            context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID), new StepCompleted(stepName, result)));
            context.addStep(stepName, StepExecution.completed(stepName, result));

            context.modifyPayload(p -> resultMapping.apply(p, result)); // write back payload
            return result;
          } catch (RuntimeException ex) {
            context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepFailed.ID), new StepFailed(stepName, ex.getMessage(), ex)));
            context.addStep(stepName, StepExecution.failed(stepName, ex));
            throw ex;
          }
        }
      ).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
      .exceptionally(ex -> {
        if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
          var timeoutTimestamp = Instant.now(context.getClock());
          context.getStateManager().append(workflowId, new GenericEventMessage(MessageType.fromString(StepTimedOut.ID), new StepTimedOut(stepName, timeoutTimestamp)));
          context.addStep(stepName, StepExecution.timedOut(stepName, timeoutTimestamp));
          throw new CompletionException(ex);
        }
        throw new StepFailedException(ex);
      });

  }
}
