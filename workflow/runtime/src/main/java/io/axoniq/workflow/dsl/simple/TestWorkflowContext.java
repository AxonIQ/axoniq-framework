package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.impl.threadsandfutures.WorkflowInstance;
import jakarta.annotation.Nonnull;

import java.time.Instant;
import java.util.Map;

public class TestWorkflowContext extends WorkflowInstance
  implements TestExecute, TestWaitForEvent // DSL customizations
{
  public TestWorkflowContext(
    @Nonnull String workflowId,
    @Nonnull Map<String, Object> payload,
    @Nonnull Instant startTime,
    @Nonnull WorkflowServices workflowServices) {
    super(workflowId, payload, startTime, workflowServices);
  }
}
