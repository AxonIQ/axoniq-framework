package io.axoniq.workflow.runtime.api.primitives;

import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;

import java.util.Map;

/**
 * Customizes workflow event names.
 */
public interface EventNameCustomizer {
  String getEventName(String stepName, Map<String, Object> parameters, StepStatus stepStatus);
  String getEventName(String stepName, Map<String, Object> parameters, WorkflowStatus stepStatus);
}
