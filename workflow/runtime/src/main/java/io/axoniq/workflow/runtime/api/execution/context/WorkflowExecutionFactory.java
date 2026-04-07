package io.axoniq.workflow.runtime.api.execution.context;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

/**
 * Factory to create a workflow execution.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
@FunctionalInterface
public interface WorkflowExecutionFactory {

    /**
     * Creates workflow execution for a given workflow context.
     *
     * @param context context to create the workflow execution for.
     * @return workflow execution.
     */
    @Nonnull
    WorkflowExecution create(@Nonnull WorkflowContext context);
}
