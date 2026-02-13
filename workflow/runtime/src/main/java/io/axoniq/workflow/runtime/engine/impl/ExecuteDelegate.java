package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.WorkflowStepResults;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.util.ContextUtils;
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
                         @Nonnull WorkflowServices workflowServices,
                         @Nonnull EventNameCustomizer parentEventNameCustomizer
  ) {
    super(context, workflowState, workflowServices, parentEventNameCustomizer);
  }

  @Override
  public WorkflowStepResult execute(
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
        workflowState.awaitStateChange(s -> s.containsStep(stepName) && s.getStep(stepName).status() == StepStatus.STARTED);
      } catch (InterruptedException e) {
        return WorkflowStepResults.failed(e);
      }
    }

    // FIXME -> consider to use QOS (at least once/at most once)
    if (workflowState.getStep(stepName).status() == StepStatus.STARTED) {
      var actualStartTime = workflowState.getStep(stepName).timestamp();
      var remainingTimeout = Duration.between(Instant.now(workflowServices.getClock()), actualStartTime.plus(timeout));
      // FIXME - This is where we capture our current consistency marker

      var result = workflowServices.getUnitOfWorkFactory()
        .create(stepName, customize -> customize.workScheduler(workflowServices.getExecutor())) // FIXME -> define a new thread pool for execution customer code
        .executeWithResult(processingContext -> {
          // FIXME - this procContext should be given to the user's input in the DSL so that they can get resource or add lifecycle phase shit
          var procContext = ContextUtils.copyResources(workflowState.getStep(stepName).context(), processingContext);
          var payload = parameterMapping.apply(workflowContext.getPayload(), local);
          return CompletableFuture.completedFuture(action.apply(procContext, payload));
        });


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
                workflowState.appendTask(i -> {
                  timedOut(stepName, workflowServices.getClock().instant(), eventNameCustomizer);
                });
              } else if (e instanceof InterruptedException) {
                // FIXME - This is where we should publish using an append condition
                workflowState.appendTask(i -> {
                  cancelled(stepName, eventNameCustomizer);
                });
              } else {
                // FIXME - This is where we should publish using an append condition
                workflowState.appendTask(i -> {
                  failed(stepName, e, eventNameCustomizer);
                });
              }
            }
          });
      }
    }

    return WorkflowStepResults.stateBased(stepName, workflowState);
  }
}
