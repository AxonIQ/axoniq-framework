package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.api.primitives.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.primitives.WaitForPrimitive;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
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

  String getWorkflowId();

  Map<String, Object> getPayload();

  void applyPayloadModification(PayloadModification payloadModification);

  WorkflowStatus getStatus();

  List<String> getStepHistory();

  ProcessingContext processingContext();
}
