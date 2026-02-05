package io.axoniq.workflow.runtime.engine.impl.taskqueue;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.StateBasedStepExecutionResult;
import io.axoniq.workflow.runtime.engine.result.StepExecutionResults;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecuteDelegate extends AbstractStepExecutor implements ExecutePrimitive {

  private static final Logger logger = LoggerFactory.getLogger(ExecuteDelegate.class);

  public ExecuteDelegate(@Nonnull WorkflowContext context,
                         @Nonnull WorkflowState workflowState,
                         @Nonnull WorkflowServices workflowServices
  ) {
    super(context, workflowState, workflowServices);
  }

  @Override
  public StepExecutionResult execute(
    @NotNull String stepName,
    @Nullable Map<String, Object> local,
    @NotNull PayloadProcessor action,
    @NotNull PayloadReducer parameterMapping,
    @NotNull PayloadReducer resultMapping,
    @NotNull Duration timeout,
    @NotNull EventNameCustomizer eventNameCustomizer
  ) {
    logger.trace("Execute {} called from thread {}", stepName, Thread.currentThread());

    acceptAllPendingTasksForStep(stepName);

    if (!workflowState.containsStep(stepName)) {
      workflowState.appendTask(i ->
        started(stepName, local, eventNameCustomizer)
      );
      try {
        workflowState.runNextStateChange(s -> s.containsStep(stepName) && s.getStep(stepName).status() == StepStatus.STARTED);
      } catch (InterruptedException e) {
        return StepExecutionResults.failed(e);
      }
    }

    if (workflowState.getStep(stepName).status() == StepStatus.STARTED) {
      var actualStartTime = workflowState.getStep(stepName).timestamp();
      var remainingTimeout = Duration.between(Instant.now(workflowServices.getClock()), actualStartTime.plus(timeout));
      // FIXME - This is where we capture our current consistency marker
      var result = CompletableFuture.supplyAsync(
        () -> {
          var payload = parameterMapping.apply(workflowContext.getPayload(), local);
          return action.apply(payload);
        }, workflowServices.getExecutor()
      );
      if (remainingTimeout.isNegative()) {
        workflowState.appendTask(i -> {
          // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
          // FIXME - This is where we should publish using an append condition
          timedOut(stepName, workflowServices.getClock().instant(), eventNameCustomizer);
        });
      } else {
        result
          .orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
          .whenComplete((r, e) -> {
            if (r != null) {
              workflowState.appendTask(i -> {
                workflowContext.applyPayloadModification(p -> resultMapping.apply(p, r)); // write back payload
                // FIXME - This is where we should publish using an append condition
                completed(stepName, r, eventNameCustomizer);
              });
            } else {
              if (e instanceof TimeoutException || e.getCause() instanceof TimeoutException) {
                // FIXME - This is where we should publish using an append condition
                timedOut(stepName, workflowServices.getClock().instant(), eventNameCustomizer);
                // FIXME Condition 1?
                /*
                workflowState.appendTask(i -> {
                  timedOut(stepName, workflowServices.getClock().instant(), eventNameCustomizer);
                });*/
              } else if (e instanceof InterruptedException) {
                // FIXME - This is where we should publish using an append condition
                // FIXME see Condition 1
                cancelled(stepName, eventNameCustomizer);
              } else {
                // FIXME - This is where we should publish using an append condition
                // FIXME see Condition 1
                failed(stepName, e, eventNameCustomizer);
              }
            }
          });
      }
    }

    return new StateBasedStepExecutionResult(stepName, () -> {
      workflowState.runNextStateChange(s -> true); // FIXME -> can we do better and provide conditions direct from the result then?
      return null;
    }, this.workflowState);
  }
}
