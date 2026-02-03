package io.axoniq.workflow.runtime.engine.impl.multi;

import io.axoniq.workflow.runtime.api.primitives.*;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.StepExecutionResults;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

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
    Function<Instant, StepExecutionResult> timeoutOccurredHandler = (timeoutTimestamp) -> {
      timedOut(stepName, timeoutTimestamp, eventNameCustomizer);
      return StepExecutionResults.timeout(timeout);
    };

    Function<Duration, StepExecutionResult> execute = (remainingTimeout) ->
      StepExecutionResults.fromFuture(
        workflowServices.getTaskManager()
          .execute(
            workflowContext.getWorkflowId(), action, parameterMapping.apply(workflowContext.getPayload(), local)
          )
          .thenApply(result -> {
              completed(stepName, result, eventNameCustomizer);
              workflowContext.applyPayloadModification(p -> resultMapping.apply(p, result)); // write back payload
              return StepExecutionResults.completed(result);
            }
          ).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
          .exceptionally(ex -> {
            if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) { // FIXME: Make sure we really unwind all
              return timeoutOccurredHandler.apply(Instant.now(workflowServices.getClock()));
            } else if (ex instanceof InterruptedException) {
              // FIXME report cancelled step !!!
              return StepExecutionResults.cancelled();
            }
            failed(stepName, ex, eventNameCustomizer);
            return StepExecutionResults.failed(ex);
          })
      );

    var existing = workflowState.getStep(stepName);
    if (existing != null) {
      switch (existing.status()) {
        case COMPLETED -> {
          @SuppressWarnings("unchecked")
          var result = (Map<String, Object>) existing.result();
          workflowContext.applyPayloadModification(p -> resultMapping.apply(p, result)); // reduce results back
          return StepExecutionResults.completed(result);
        }
        case FAILED -> {
          return StepExecutionResults.failed(existing.error());
        }
        case TIMED_OUT -> {
          return StepExecutionResults.timeout(timeout);
        }
        case STARTED -> {
          var startedPayload = (Map<?, ?>) existing.result();
          Instant startedAt = (Instant) startedPayload.get("started");
          Duration startedDuration = (Duration) startedPayload.get("duration");
          if (startedDuration != timeout) {
            // FIXME
            // the timeout duration has changed => instance migration?
          }
          Duration remainingTimeout = Duration.between(Instant.now(workflowServices.getClock()), startedAt.plus(timeout));
          if (remainingTimeout.isNegative()) {
            return timeoutOccurredHandler.apply(Instant.now(workflowServices.getClock()));
          } else {
            // execute with remaining
            return execute.apply(remainingTimeout);
          }
        }
      }
    }

    started(
      stepName,
      withValue(local,
        "started", Instant.now(workflowServices.getClock()),
        "duration", timeout
      ),
      eventNameCustomizer);
    return execute.apply(timeout);
  }

  Map<String, Object> withValue(Map<String, Object> map, String key, Object value, String key2, Object value2) {
    var result = new HashMap<>(map);
    result.put(key, value);
    result.put(key2, value2);
    return result;
  }
}
