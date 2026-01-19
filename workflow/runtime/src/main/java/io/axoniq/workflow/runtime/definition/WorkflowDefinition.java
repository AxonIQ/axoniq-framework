package io.axoniq.workflow.runtime.definition;

public interface WorkflowDefinition {

    Workflow create();

    // TODO - Add method to recreate state based on ExecutionContext
}
