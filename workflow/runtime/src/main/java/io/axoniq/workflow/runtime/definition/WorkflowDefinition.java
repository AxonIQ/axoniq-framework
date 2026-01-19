package io.axoniq.workflow.runtime.definition;

import io.axoniq.workflow.runtime.context.WorkflowContext;

import java.util.Map;

public interface WorkflowDefinition {
    void execute(WorkflowContext context, Map<String, Object> args);
}
