package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.dsl.simple.TestExecute;
import io.axoniq.workflow.dsl.simple.TestWaitForEvent;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.impl.single.WorkflowInstance;
import jakarta.annotation.Nonnull;

import java.util.Map;

public class OtherWorkflowContext extends WorkflowInstance
  implements TestExecute, TestWaitForEvent // DSL customizations
{
  public OtherWorkflowContext(
    @Nonnull String workflowId,
    @Nonnull Map<String, Object> payload,
    @Nonnull WorkflowServices workflowServices) {
    super(workflowId, payload, workflowServices);
  }
}
