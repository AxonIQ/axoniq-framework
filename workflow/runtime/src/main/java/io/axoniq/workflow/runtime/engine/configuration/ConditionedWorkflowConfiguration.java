package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;

public record ConditionedWorkflowConfiguration<C extends WorkflowContext>(
        EventCondition eventCondition,
        WorkflowConfiguration<C> workflowConfiguration
) {

}
