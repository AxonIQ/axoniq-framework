package io.axoniq.workflow.runtime.api;

import java.util.function.Consumer;

/**
 * Definition of the workflow.
 *
 * @param <T>
 */
@FunctionalInterface
public interface WorkflowDefinition<T extends WorkflowContext> extends Consumer<T> {
}
