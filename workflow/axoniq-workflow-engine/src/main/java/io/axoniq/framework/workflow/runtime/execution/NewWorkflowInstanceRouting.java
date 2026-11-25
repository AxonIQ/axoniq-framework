/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.WorkflowIds;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Static helpers that decide whether, and under which workflow id, a new workflow instance starts under a multi-version
 * registry.
 * <p>
 * Lives outside {@link WorkflowEngine} because the decision is pure routing over the workflows in memory.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
final class NewWorkflowInstanceRouting {

    private static final Logger logger = LoggerFactory.getLogger(NewWorkflowInstanceRouting.class);

    private NewWorkflowInstanceRouting() {
    }

    /**
     * Reports whether the given configuration's {@code workflowIdProvider} derived a workflow id from the given event,
     * logging the misconfiguration that produced no id and skipping the new instance when it did not.
     * <p>
     * A provider deriving no id (an {@code idProperty} naming a property the event does not carry, typically) is never
     * transient: every event of that type derives no id again. Failing the start would fail the work package and stall
     * the segment processing it, taking down every instance that segment owns instead of just the misconfigured
     * definition; skipping the start keeps the damage to that one definition, and naming the definition and the event
     * here keeps it attributable. The provider is an arbitrary function over the event that resolves property names
     * against the converted payload, so registration cannot reject it up front.
     *
     * @param baseWorkflowId        the id the provider derived, or {@code null} when it derived none
     * @param workflowConfiguration configuration whose provider was asked
     * @param eventMessage          event the id was to be derived from
     * @return {@code true} when an id was derived and the new instance may start
     */
    public static boolean hasDerivedWorkflowId(
            @Nullable String baseWorkflowId,
            WorkflowConfiguration<?> workflowConfiguration,
            EventMessage eventMessage
    ) {
        if (baseWorkflowId != null) {
            return true;
        }
        logger.error(
                "The workflowIdProvider ({}) of workflow '{}' version '{}' derived no workflow id from event '{}'; "
                        + "not starting an instance. Check that the configured idProperty names a property the event "
                        + "actually carries.",
                workflowConfiguration.workflowIdProvider().getClass().getName(),
                workflowConfiguration.workflowName(),
                workflowConfiguration.workflowVersion(),
                eventMessage.type().qualifiedName()
        );
        return false;
    }

    /**
     * Reports whether the given workflow id is valid, see {@link WorkflowIds}, logging the invalid id and skipping the
     * new instance when it is not.
     * <p>
     * Like an id that cannot be derived, an invalid id is never transient: the same event derives it again. Failing the
     * start would fail the work package and stall the segment processing it, so the start is skipped instead, keeping
     * the damage to that one instance.
     *
     * @param workflowId            the id the provider derived
     * @param workflowConfiguration configuration whose provider derived the id
     * @param eventMessage          event the id was derived from
     * @return {@code true} when the id is valid and the new instance may start
     */
    public static boolean hasValidWorkflowId(
            String workflowId,
            WorkflowConfiguration<?> workflowConfiguration,
            EventMessage eventMessage
    ) {
        var invalidity = WorkflowIds.invalidity(workflowId);
        if (invalidity.isEmpty()) {
            return true;
        }
        logger.error(
                "The workflowIdProvider ({}) of workflow '{}' version '{}' derived an invalid workflow id from event "
                        + "'{}': {}; not starting an instance. A workflow id has at least one and at most {} "
                        + "characters, all printable ASCII. The id starts with '{}'.",
                workflowConfiguration.workflowIdProvider().getClass().getName(),
                workflowConfiguration.workflowName(),
                workflowConfiguration.workflowVersion(),
                eventMessage.type().qualifiedName(),
                invalidity.get(),
                WorkflowIds.MAX_LENGTH,
                WorkflowIds.abbreviate(workflowId)
        );
        return false;
    }

    /**
     * Decides the workflow id to use for a new start given the execution in memory under the base id, if any:
     * <ul>
     *   <li>No existing execution at {@code baseWorkflowId} → return the base id unchanged.</li>
     *   <li>Existing execution at {@code baseWorkflowId}, at any version → reject the start (returns {@code null})
     *       and warn. Workflow ids MUST be unique, so the version never becomes part of the id.</li>
     * </ul>
     *
     * @param existing           the execution in memory under {@code baseWorkflowId}, or empty when there is none
     * @param baseWorkflowId     the id the workflow id provider derived for the new instance
     * @param newInstanceVersion the version of the definition the new instance would start at
     * @param eventMessage       the event that would start the new instance
     * @return the workflow id of the new instance, or {@code null} when the start is rejected as a duplicate
     */
    @Nullable
    public static String resolveWorkflowIdForNewInstance(
            Optional<WorkflowExecution> existing,
            String baseWorkflowId,
            String newInstanceVersion,
            EventMessage eventMessage
    ) {
        if (existing.isEmpty()) {
            return baseWorkflowId;
        }
        var existingVersion = existing.get().state().workflowDefinitionId().version();
        logger.warn(
                "A workflow with id '{}' is already running at version '{}'; ignoring new start request at version "
                        + "'{}' triggered by event '{}'. Workflow ids MUST be unique, so the start is treated as a "
                        + "duplicate. To run two instances in parallel, use a different idProperty value.",
                baseWorkflowId, existingVersion, newInstanceVersion, eventMessage.type().qualifiedName()
        );
        return null;
    }
}
