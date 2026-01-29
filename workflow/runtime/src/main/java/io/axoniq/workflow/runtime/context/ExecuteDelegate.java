package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.primitives.StepResult;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecuteDelegate extends AbstractStepExecutor implements ExecutePrimitive {

  public ExecuteDelegate(@Nonnull WorkflowContext context,
                         @Nonnull WorkflowState workflowState,
                         @Nonnull WorkflowServices workflowServices
  ) {
    super(context, workflowState, workflowServices);
  }

  @Override
  public CompletableFuture<StepResult> execute(
    @Nonnull String stepName,
    @Nullable Map<String, Object> local,
    @Nonnull PayloadProcessor action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {
    var existing = workflowState.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          @SuppressWarnings("unchecked")
          var result = (Map<String, Object>) existing.result();
          workflowState.applyPayloadModification(p -> resultMapping.apply(p, result)); // reduce results back
          return CompletableFuture.completedFuture(CompletedStepResult.completed(result));
        }
        case FAILED -> {
          return CompletableFuture.completedFuture(CompletedStepResult.failed(existing.error()));
        }
        case TIMED_OUT -> {
          return CompletableFuture.completedFuture(CompletedStepResult.timeout(timeout));
        }
      }
    }

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            started(stepName, local, eventNameCustomizer);
            // step execution
            var parameters = parameterMapping.apply(workflowContext.getPayload(), local); // local copy of the payload
            Map<String, Object> result = action.apply(parameters);

            completed(stepName, result, eventNameCustomizer);

            workflowState.applyPayloadModification(p -> resultMapping.apply(p, result)); // write back payload
            return CompletedStepResult.completed(result);
          } catch (RuntimeException ex) {
            failed(stepName, ex, eventNameCustomizer);
            return CompletedStepResult.failed(ex);
          }
        }, workflowServices.getExecutor()
      ).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
      .exceptionally(ex -> {
        if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) { // FIXME: Make sure we really unwinded all
          timedOut(stepName, eventNameCustomizer);
          return CompletedStepResult.timeout(timeout);
        }
        return CompletedStepResult.failed(ex);
      });

  }
}
