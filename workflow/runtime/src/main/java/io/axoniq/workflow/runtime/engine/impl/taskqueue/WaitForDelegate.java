package io.axoniq.workflow.runtime.engine.impl.taskqueue;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.StateBasedStepExecutionResult;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
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

    // Process pending tasks
    while ((!workflowState.containsStep(stepName) && !workflowState.hasTasks()) || !workflowState.isExecutable()) {
      var poll = workflowState.getNextTask();
      if (poll != null) {
        poll.accept(this.workflowState);
      }
    }

    var instance = (WorkflowInstance) workflowState;

    if (!workflowState.containsStep(stepName)) {
      //Start time of wait will be last completed timestamp of any step or start time of workflow if no steps
      Instant lastStepTimestamp = instance.getLastStepTimestamp();
      workflowState.addStep(StepExecution.started(stepName, null, lastStepTimestamp));

      // Register wait condition
      instance.registerWaitCondition(stepName, qualifiedName, predicate);

      // Compute remaining time
      Duration elapsed = Duration.between(lastStepTimestamp, Instant.now(workflowServices.getClock()));
      Duration remaining = timeout.minus(elapsed);

      // Schedule delayed timeout task
      if (remaining.isNegative() || remaining.isZero()) {
        workflowState.addStep(StepExecution.timedOut(stepName, null, lastStepTimestamp.plus(timeout)));
        instance.removeWaitCondition(stepName);
      } else {
        CompletableFuture.runAsync(() ->
          workflowState.appendTask(i -> {
            if (i.getStep(stepName) != null && i.getStep(stepName).status() == StepStatus.STARTED) {
              i.addStep(StepExecution.timedOut(stepName, null, Instant.now(workflowServices.getClock())));
              instance.removeWaitCondition(stepName);
            }
          }),
          CompletableFuture.delayedExecutor(remaining.toMillis(), TimeUnit.MILLISECONDS)
        );
      }
    }

    return new StateBasedStepExecutionResult(stepName, () -> {
      workflowState.runNextStateChange();
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
