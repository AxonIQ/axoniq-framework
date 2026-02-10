package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.WorkflowStepResults;
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
import java.util.function.Predicate;

public class WaitForDelegate extends AbstractStepExecutor implements WaitForPrimitive {

  private static final Logger logger = LoggerFactory.getLogger(WaitForDelegate.class);

  public WaitForDelegate(
    @Nonnull WorkflowContext workflowContext,
    @Nonnull WorkflowState workflowState,
    @Nonnull WorkflowServices workflowServices,
    @Nonnull EventNameCustomizer parentCustomizer) {
    super(workflowContext, workflowState, workflowServices, parentCustomizer);
  }

  @Override
  public WorkflowStepResult waitFor(
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
        workflowState.awaitStateChange(s -> s.containsStep(stepName) && s.getStep(stepName).status() == StepStatus.STARTED);
      } catch (InterruptedException e) {
        return WorkflowStepResults.failed(e);
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
          if (!i.getStep(stepName).status().isTerminal()) {
            // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
            // FIXME - This is where we should publish using an append condition
            timedOut(stepName, workflowServices.getClock().instant(), eventNameCustomizer);
          }
        });

      } else {
        // Register wait condition
        workflowState.registerWaitCondition(stepName, qualifiedName, predicate, eventNameCustomizer);
        CompletableFuture.runAsync(() -> {
            workflowState.removeWaitCondition(stepName);
            workflowState.appendTask(i -> {
                if (!i.getStep(stepName).status().isTerminal()) {
                  // only timeout if we are not completed yet
                  timedOut(stepName, eventNameCustomizer);
                }
              }
            );
          }, CompletableFuture.delayedExecutor(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
        ).exceptionally(e -> {
          if (e instanceof InterruptedException) {
            workflowState.removeWaitCondition(stepName);
            workflowState.appendTask(i -> {
              // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
              // FIXME - This is where we should publish using an append condition
              if (!i.getStep(stepName).status().isTerminal()) {
                cancelled(stepName, eventNameCustomizer);
              }
            });
          }
          return null;
        });
      }
    }

    return WorkflowStepResults.stateBased(stepName, workflowState);
  }
}
