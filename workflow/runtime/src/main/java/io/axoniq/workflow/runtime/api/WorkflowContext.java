package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.List;
import java.util.Map;

/**
 * Public API facing class to access the execution from workflow definition.
 */
public interface WorkflowContext extends
  ExecutePrimitive,
  WaitForPrimitive,
  DescribableComponent {

  @Nonnull
  String getWorkflowId();

  @Nonnull
  Map<String, Object> getPayload();

  void applyPayloadModification(@Nonnull PayloadModification payloadModification);

  @Nonnull
  WorkflowStatus getStatus();

  @Nonnull
  List<String> getStepHistory();

  @Nonnull
  ProcessingContext processingContext();
}
