package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
  public StepExecutionResult execute(
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
          return StepExecutionResults.completed(result);
        }
        case FAILED -> {
          return StepExecutionResults.failed(existing.error());
        }
        case TIMED_OUT -> {
          return StepExecutionResults.timeout(timeout);
        }
      }
    }

    // FIXME -> Shift away from the primitive!!!
    return StepExecutionResults.fromFuture(CompletableFuture.supplyAsync(
        () -> {
          try {
            started(stepName, local, eventNameCustomizer);
            // step execution
            var parameters = parameterMapping.apply(workflowContext.getPayload(), local); // local copy of the payload
            Map<String, Object> result = action.apply(parameters);

            completed(stepName, result, eventNameCustomizer);

            workflowState.applyPayloadModification(p -> resultMapping.apply(p, result)); // write back payload
            return StepExecutionResults.completed(result);
          } catch (RuntimeException ex) {
            failed(stepName, ex, eventNameCustomizer);
            return StepExecutionResults.failed(ex);
          }
        }, workflowServices.getExecutor() // => FIXME This is not the right place to decide
      ).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
      .exceptionally(ex -> {
        if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) { // FIXME: Make sure we really unwind all
          timedOut(stepName, eventNameCustomizer);
          return StepExecutionResults.timeout(timeout);
        }
        return StepExecutionResults.failed(ex);
      }));

  }
}
