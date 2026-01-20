package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.context.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.context.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.payload.Payload;
import io.axoniq.workflow.runtime.payload.PayloadFunction;

import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.function.Predicate;

public interface WorkflowContext extends
  ExecutePrimitive,
  WaitForPrimitive {

  default <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
    return this.waitFor(stepName, eventType, predicate, timeout).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
    return this.waitFor(stepName, eventType, (e) -> true, timeout).join();
  }

  default <T> T waitForEvent(String stepName, Class<T> eventType) {
    return waitForEvent(stepName, eventType, Duration.ofSeconds(5)); // TODO default
  }

  String getWorkflowId();

  Payload getPayload();

  StateManager getStateManager();

  void modifyPayload(PayloadFunction payloadModification);

  void addStep(String stepName, StepExecution execution);

  StepExecution getStep(String stepName);

  Set<String> getStepHistory();

  Clock getClock();
}
