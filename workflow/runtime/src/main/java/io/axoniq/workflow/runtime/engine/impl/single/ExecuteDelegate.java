package io.axoniq.workflow.runtime.engine.impl.single;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.StateBasedStepExecutionResult;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
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
    @NotNull String stepName,
    @Nullable Map<String, Object> local,
    @NotNull PayloadProcessor action,
    @NotNull PayloadReducer parameterMapping,
    @NotNull PayloadReducer resultMapping,
    @NotNull Duration timeout,
    @NotNull EventNameCustomizer eventNameCustomizer
  ) {
    while ((!workflowState.containsStep(stepName) && !workflowState.hasTasks()) || !workflowState.isExecutable()) {
      var poll = workflowState.getNextTask();
      if (poll != null) {
        poll.accept(this.workflowState);
      }
    }
    if (workflowState.getStep(stepName) == null) {
      workflowState.addStep(
        StepExecution.started(stepName, null, workflowServices.getClock().instant())
      );
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
        result.orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
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
              } else if (e instanceof InterruptedException) {
                // FIXME - This is where we should publish using an append condition
                cancelled(stepName, eventNameCustomizer);
              } else {
                // FIXME - This is where we should publish using an append condition
                failed(stepName, e, eventNameCustomizer);
              }
            }
          });
      }
    }

    return new StateBasedStepExecutionResult(stepName, () -> {
      workflowState.runNextStateChange();
      return null;
    }, this.workflowState);
  }
}
