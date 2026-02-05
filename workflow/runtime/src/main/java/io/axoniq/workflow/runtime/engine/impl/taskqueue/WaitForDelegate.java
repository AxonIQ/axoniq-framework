package io.axoniq.workflow.runtime.engine.impl.taskqueue;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.StateBasedStepExecutionResult;
import io.axoniq.workflow.runtime.engine.result.StepExecutionResults;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

public class WaitForDelegate extends AbstractStepExecutor implements WaitForPrimitive {

  private static final Logger logger = LoggerFactory.getLogger(WaitForDelegate.class);

  public WaitForDelegate(
    @Nonnull WorkflowContext workflowContext,
    @Nonnull WorkflowState workflowState,
    @Nonnull WorkflowServices workflowServices
  ) {
    super(workflowContext, workflowState, workflowServices);
  }

  @Override
  public StepExecutionResult waitFor(
    @NotNull String stepName,
    @NotNull QualifiedName qualifiedName,
    @NotNull Predicate<EventMessage> predicate,
    @NotNull Duration timeout,
    @NotNull EventNameCustomizer eventNameCustomizer
  ) {
    logger.trace("WaitFor {} called from thread {}", stepName, Thread.currentThread());

    acceptAllPendingTasksForStep(stepName);

    if (!workflowState.containsStep(stepName)) {
      workflowState.appendTask(i ->
        started(stepName, Map.of("startTime", workflowServices.getClock().instant()), eventNameCustomizer)
      );
      try {
        workflowState.runNextStateChange(s -> s.containsStep(stepName) && s.getStep(stepName).status() == StepStatus.STARTED);
      } catch (InterruptedException e) {
        return StepExecutionResults.failed(e);
      }
    }

    if (workflowState.getStep(stepName).status() == StepStatus.STARTED) {
      // Start time of wait will be last completed timestamp of any step or start time of workflow if no steps
/*
      var actualStartTime = workflowContext // TODO: discuss if we use it if no started event is there
        .getStepHistory()
        .stream()
        .map(workflowState::getStep)
        .map(StepExecution::timestamp)
        .max(Instant::compareTo)
        .orElse(workflowContext.getStartTime());

 */
      var actualStartTime = workflowState.getStep(stepName).timestamp();
      var remainingTimeout = Duration.between(Instant.now(workflowServices.getClock()), actualStartTime.plus(timeout));

      if (remainingTimeout.isNegative()) {
        workflowState.appendTask(i -> {
          // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
          // FIXME - This is where we should publish using an append condition
          timedOut(stepName, workflowServices.getClock().instant(), eventNameCustomizer);
        });

      } else {
        // Register wait condition
        workflowState.registerWaitCondition(stepName, qualifiedName, predicate, eventNameCustomizer);
        CompletableFuture.runAsync(() ->
            workflowState.appendTask(i -> {
                workflowState.removeWaitCondition(stepName);
                timedOut(stepName, eventNameCustomizer);
              }
            )
          , CompletableFuture.delayedExecutor(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
        ).exceptionally(e -> {
          if (e instanceof InterruptedException) {
            workflowState.appendTask(i -> {
              // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
              // FIXME - This is where we should publish using an append condition
              workflowState.removeWaitCondition(stepName);
              cancelled(stepName, eventNameCustomizer);
            });
          }
          return null;
        });
      }
    }

    return new StateBasedStepExecutionResult(stepName, () -> {
      workflowState.runNextStateChange(s -> true);
      return null;
    }, workflowState);
  }

  @Override
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return workflowContext.typeToPayloadConverter();
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> payloadType) {
    return workflowContext.payloadToTypeConverter(payloadType);
  }
}
