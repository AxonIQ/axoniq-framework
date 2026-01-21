package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.api.workflow.PayloadFunction;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.engine.StepExecution;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

public class WorkflowExecutionImpl implements WorkflowContext {

  private final String workflowId;
  private final Map<String, StepExecution> steps = new ConcurrentHashMap<>();
  private final StateManager stateManager;
  private Map<String, Object> global;

  // primitive implementations
  private final ExecutePrimitive executePrimitive = new ExecuteDelegate(this);
  private final WaitForPrimitive waitForPrimitive = new WaitForDelegate(this);
  private final ConversionDelegate conversionDelegate = new ConversionDelegate();

  public WorkflowExecutionImpl(String workflowId, StateManager stateManager, Map<String, Object> payload) {
    this.workflowId = workflowId;
    this.stateManager = stateManager;
    this.global = payload;
  }

  @Override
  public CompletableFuture<Map<String, Object>> execute(
    @Nonnull String stepName,
    @Nullable Map<String, Object> local,
    @Nonnull PayloadFunction action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
    return executePrimitive.execute(stepName, local, action, parameterMapping, resultMapping, timeout, eventNameCustomizer);
  }

  @Override
  public <T> CompletableFuture<T> waitFor(
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
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(Class<T> type) {
    return conversionDelegate.payloadToTypeConverter(type);
  }

  @Override
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public Map<String, Object> getPayload() {
    return global;
  }

  @Override
  public StateManager getStateManager() {
    return stateManager;
  }

  @Override
  public void modifyPayload(PayloadFunction payloadModification) {
    this.global = payloadModification.apply(global);
  }

  @Override
  public void addStep(StepExecution execution) {
    steps.put(execution.stepName(), execution);
  }

  @Override
  public StepExecution getStep(String stepName) {
    return steps.get(stepName);
  }

  @Override
  public Set<String> getStepHistory() {
    return steps.keySet();
  }

  @Override
  public Clock getClock() {
    return Clock.systemDefaultZone();
  }

  public void restoreStep(StepExecution step) {
    steps.put(step.stepName(), step);
  }
}
