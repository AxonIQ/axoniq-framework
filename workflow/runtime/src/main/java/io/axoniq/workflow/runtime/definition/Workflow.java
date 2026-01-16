package io.axoniq.workflow.runtime.definition;

import io.axoniq.workflow.runtime.WorkflowState;

public interface Workflow {

    WorkflowState initialState();

    // TODO - Add method to recreate state based on ExecutionContext
}
