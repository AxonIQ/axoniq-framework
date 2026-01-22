package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import io.axoniq.workflow.runtime.engine.StepExecution;
import io.axoniq.workflow.runtime.engine.StepFailedException;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;

public class ExecuteDelegate extends AbstractContextAwarePrimitiveDelegate implements ExecutePrimitive {

  public ExecuteDelegate(WorkflowContext context, WorkflowState workflowState) {
    super(context, workflowState);
  }

  @Override
  public CompletableFuture<Map<String, Object>> execute(
    @Nonnull String stepName,
    @Nullable Map<String, Object> local,
    @Nonnull PayloadProcessor action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {
    StepExecution existing = lifecycle.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          @SuppressWarnings("unchecked")
          var result = (Map<String, Object>) existing.result();
          lifecycle.modifyPayload(p -> resultMapping.apply(p, result)); // reduce results back
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

    return CompletableFuture.supplyAsync(
        () -> {
          try {

            lifecycle.getStateManager().append(startedStep(context, stepName, local, eventNameCustomizer));
            lifecycle.addStep(StepExecution.started(stepName, Instant.now(lifecycle.getClock())));

            // step execution
            var parameters = parameterMapping.apply(context.getPayload(), local); // local copy of the payload
            Map<String, Object> result = action.apply(parameters);

            lifecycle.getStateManager().append(completedStep(context, stepName, result, eventNameCustomizer));
            lifecycle.addStep(StepExecution.completed(stepName, result));

            lifecycle.modifyPayload(p -> resultMapping.apply(p, result)); // write back payload
            return result;
          } catch (RuntimeException ex) {
            lifecycle.getStateManager().append(failStep(context, stepName, ex, eventNameCustomizer));
            lifecycle.addStep(StepExecution.failed(stepName, ex));
            throw ex;
          }
        }
      ).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
      .exceptionally(ex -> {
        if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
          var timeoutTimestamp = Instant.now(lifecycle.getClock());
          lifecycle.getStateManager().append(timeoutStep(context, stepName, timeoutTimestamp, eventNameCustomizer));
          lifecycle.addStep(StepExecution.timedOut(stepName, timeoutTimestamp));
          throw new CompletionException(ex);
        }
        throw new StepFailedException(ex);
      });

  }
}
