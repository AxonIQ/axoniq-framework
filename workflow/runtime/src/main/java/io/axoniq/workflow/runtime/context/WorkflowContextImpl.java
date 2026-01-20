package io.axoniq.workflow.runtime.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.api.PayloadFunction;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.engine.StepExecution;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public class WorkflowContextImpl implements WorkflowContext {

  private final String workflowId;
  private final Map<String, StepExecution> steps = new LinkedHashMap<>();
  private final StateManager stateManager;
  private Map<String, Object> global;
  private final ObjectMapper objectMapper = new ObjectMapper();

  // primitive implementations
  private final ExecutePrimitive executePrimitive = new ExecuteDelegate(this);
  private final WaitForPrimitive waitForPrimitive = new WaitForDelegate(this);

  public WorkflowContextImpl(String workflowId, StateManager stateManager, Map<String, Object> payload) {
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
    @Nonnull Duration timeout
    ) {
    return executePrimitive.execute(stepName, local, action, parameterMapping, resultMapping, timeout);
  }

  @Override
  public <T> CompletableFuture<T> waitFor(
    @Nonnull String stepName,
    @Nonnull Class<T> eventType,
    @Nonnull Predicate<T> predicate,
    @Nonnull Duration timeout,
    @Nonnull Function<T, Map<String, Object>> converter
  ) {
    return waitForPrimitive.waitFor(stepName, eventType, predicate, timeout, converter);
  }

  @Override
  public Function<Object, Map<String, Object>> getDefaultPayloadProjector() {
    return (t) -> objectMapper.convertValue(t, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
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
  public void addStep(String stepName, StepExecution execution) {
    steps.put(stepName, execution);
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
