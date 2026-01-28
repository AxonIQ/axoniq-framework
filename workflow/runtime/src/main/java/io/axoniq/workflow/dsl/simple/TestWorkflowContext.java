package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.engine.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowInstance;
import jakarta.annotation.Nonnull;

import java.util.Map;

public class TestWorkflowContext extends WorkflowInstance
  implements TestExecute, TestWaitForEvent // DSL customizations
{
  public TestWorkflowContext(
    @Nonnull String workflowId,
    @Nonnull Map<String, Object> payload,
    @Nonnull WorkflowServices workflowServices) {
    super(workflowId, payload, workflowServices);
  }
}
