package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Map;

/**
 * Customizes workflow event names.
 */
public interface EventNameCustomizer {
  @Nonnull
  QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters, @Nonnull StepStatus stepStatus);
  @Nonnull
  QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters, @Nonnull WorkflowStatus stepStatus);

  @Nonnull
  EventNameCustomizer forStepInheritance();
}
