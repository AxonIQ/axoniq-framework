package io.axoniq.workflow.runtime.api.primitives;

import io.axoniq.workflow.runtime.engine.StepStatus;

import java.util.Map;

public interface EventNameCustomizer {
  String getEventName(String stepName, Map<String, Object> parameters, StepStatus stepStatus);
}
