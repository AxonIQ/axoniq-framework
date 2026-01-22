package io.axoniq.workflow.runtime.api.primitives;

import io.axoniq.workflow.runtime.engine.StepStatus;
import io.axoniq.workflow.runtime.engine.WorkflowStatus;

import java.util.Map;

public interface EventNameCustomizer {
  String getEventName(String stepName, Map<String, Object> parameters, StepStatus stepStatus);
  String getEventName(String stepName, Map<String, Object> parameters, WorkflowStatus stepStatus);
}
