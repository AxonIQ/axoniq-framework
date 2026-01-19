package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.context.primitives.ExecuteDelegate;
import io.axoniq.workflow.runtime.context.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.context.primitives.WaitForDelegate;
import io.axoniq.workflow.runtime.context.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.payload.Payload;
import io.axoniq.workflow.runtime.payload.PayloadFunction;
import io.axoniq.workflow.runtime.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class WorkflowContextImpl implements WorkflowContext {

  private final String workflowId;
  private final Map<String, StepExecution> steps = new LinkedHashMap<>();
  private final StateManager stateManager;
  private Payload global;

  // primitive implementations
  private final ExecutePrimitive executePrimitive = new ExecuteDelegate(this);
  private final WaitForPrimitive waitForPrimitive = new WaitForDelegate(this);

  public WorkflowContextImpl(String workflowId, StateManager stateManager, Payload payload) {
    this.workflowId = workflowId;
    this.stateManager = stateManager;
    this.global = payload;
  }

  @Override
  public Payload execute(
    @Nonnull String stepName,
    @Nullable Payload local,
    @Nonnull PayloadFunction action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping
  ) {
    return executePrimitive.execute(stepName, local, action, parameterMapping, resultMapping);
  }

  @Override
  public <T> T waitFor(String stepName, Class<T> eventType, Duration timeout, Predicate<T> predicate, TimeoutMode timoutMode) {
    return waitForPrimitive.waitFor(stepName, eventType, timeout, predicate, timoutMode);
  }


  @Override
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public Payload getPayload() {
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

  public void restoreStep(StepExecution step) {
    steps.put(step.stepName(), step);
  }
}
