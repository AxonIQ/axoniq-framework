package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.primitives.*;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.context.ConversionDelegate;
import io.axoniq.workflow.runtime.context.ExecuteDelegate;
import io.axoniq.workflow.runtime.context.WaitForDelegate;
import io.axoniq.workflow.runtime.engine.WorkflowServices;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import io.axoniq.workflow.runtime.engine.streaming.EventSubscriptionManager;
import io.axoniq.workflow.runtime.engine.streaming.WorkflowEventAppender;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.util.MetadataUtils.*;

public class WorkflowInstance implements WorkflowContext, WorkflowState {

  private final String workflowId;
  private final Map<String, StepExecution> steps = new ConcurrentHashMap<>();
  private Map<String, Object> payload;
  private WorkflowStatus status = WorkflowStatus.NONE;

  private final transient Clock clock;

  // primitive implementations
  private final transient ExecutePrimitive executePrimitive;
  private final transient WaitForPrimitive waitForPrimitive;
  private final transient ConversionDelegate conversionDelegate = new ConversionDelegate();


  public WorkflowInstance(
    @Nonnull String workflowId,
    @Nonnull Map<String, Object> payload,
    @Nonnull WorkflowServices workflowServices
    ) {
    this.workflowId = workflowId;
    this.payload = payload;
    this.clock = workflowServices.getClock();
    this.executePrimitive = new ExecuteDelegate(this, this, workflowServices);
    this.waitForPrimitive = new WaitForDelegate(this, this, workflowServices);
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
    return executePrimitive.execute(stepName, local, action, parameterMapping, resultMapping, timeout, eventNameCustomizer);
  }

  @Override
  public <T> CompletableFuture<StepResult> waitFor(
    @Nonnull String stepName,
    @Nonnull Class<T> eventType,
    @Nonnull Predicate<T> predicate,
    @Nonnull Duration timeout,
    @Nonnull Function<T, Map<String, Object>> converter,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {
    return waitForPrimitive.waitFor(stepName, eventType, predicate, timeout, converter, eventNameCustomizer);
  }

  @Override
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return conversionDelegate.typeToPayloadConverter();
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> type) {
    return conversionDelegate.payloadToTypeConverter(type);
  }

  @Override
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public Map<String, Object> getPayload() {
    return payload;
  }

  @Override
  public void applyPayloadModification(PayloadProcessor payloadModification) {
    this.payload = Objects.requireNonNull(payloadModification.apply(payload), "Payload must not be null");
  }

  @Override
  public StepExecution getStep(String stepName) {
    return steps.get(stepName);
  }

  @Override
  public Clock getClock() {
    return clock;
  }

  @Override
  public WorkflowStatus getStatus() {
    return status;
  }

  @Override
  public List<String> getStepHistory() {
    return steps.values().stream().map(StepExecution::stepName).toList();
  }

  @Override
  public void onEvent(EventMessage eventMessage, ProcessingContext processingContext) {
    Object eventPayload = eventMessage.payloadAs(Object.class);
    var metadata = eventMessage.metadata();
    // Apply step-level state changes
    getStepStatus(metadata).ifPresent(stepStatus -> {
      var stepName = getStepName(metadata);
      switch (stepStatus) {
        case STARTED:
          addStep(StepExecution.started(stepName, eventPayload));
          break;
        case FAILED:
          addStep(StepExecution.failed(stepName, (Throwable) eventPayload));
          break;
        case TIMED_OUT:
          addStep(StepExecution.timedOut(stepName, eventPayload));
          break;
        case COMPLETED:
          addStep(StepExecution.completed(stepName, eventPayload));
          break;
        default:
          break;
      }
    });

    // Apply workflow-level state changes
    getWorkflowStatus(metadata).ifPresent(status ->
      this.status = status
    );
  }

  private void addStep(StepExecution execution) {
    steps.put(execution.stepName(), execution);
  }
}
