package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowState;
import io.axoniq.workflow.runtime.engine.StepExecution;
import io.axoniq.workflow.runtime.engine.StepFailedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecuteDelegate extends AbstractPrimitiveDelegate implements ExecutePrimitive {

  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

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
    StepExecution existing = state.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          @SuppressWarnings("unchecked")
          var result = (Map<String, Object>) existing.result();
          state.modifyPayload(p -> resultMapping.apply(p, result)); // reduce results back
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

            started(stepName, local, eventNameCustomizer);

            // step execution
            var parameters = parameterMapping.apply(context.getPayload(), local); // local copy of the payload
            Map<String, Object> result = action.apply(parameters);

            completed(stepName, result, eventNameCustomizer);

            state.modifyPayload(p -> resultMapping.apply(p, result)); // write back payload
            return result;
          } catch (RuntimeException ex) {
            failed(stepName, ex, eventNameCustomizer);
            throw ex;
          }
        }, executor //todo reuse execute from workflow engine
      ).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
      .exceptionally(ex -> {
        if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
          timedOut(stepName, eventNameCustomizer);
          throw new CompletionException(ex);
        }
        throw new StepFailedException(ex);
      });

  }

}
