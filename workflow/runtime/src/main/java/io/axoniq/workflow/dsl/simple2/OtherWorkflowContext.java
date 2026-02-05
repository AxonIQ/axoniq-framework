package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.impl.taskqueue.WorkflowInstance;
import jakarta.annotation.Nonnull;

import java.time.Instant;
import java.util.Map;

public class OtherWorkflowContext extends WorkflowInstance
  implements OtherExecute, OtherWaitForEvent // DSL customizations
{
  public OtherWorkflowContext(
    @Nonnull String workflowId,
    @Nonnull Map<String, Object> payload,
    @Nonnull Instant startTime,
    @Nonnull WorkflowServices workflowServices) {
    super(workflowId, payload, startTime, workflowServices);
  }
}
