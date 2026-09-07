/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static helpers that decide whether, and under which workflow id, a new workflow instance starts under a multi-version
 * registry.
 * <p>
 * Lives outside {@link WorkflowEngine} because the decision is pure routing over the repository state.
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
     * Decides the workflow id to use for a new start given an existing execution repository:
     * <ul>
     *   <li>No existing execution at {@code baseWorkflowId} → return the base id unchanged.</li>
     *   <li>Existing execution at the <em>same</em> version → reject the start (returns {@code null})
     *       and warn.</li>
     *   <li>Existing execution at a <em>different</em> version → return {@code <base>#<version>} so
     *       both versions coexist in parallel.</li>
     *   <li>The disambiguated id is also already taken → reject and warn.</li>
     * </ul>
     */
    @Nullable
    public static String resolveWorkflowIdForNewInstance(
            WorkflowExecutionRepository repository,
            String baseWorkflowId,
            String newInstanceVersion,
            EventMessage eventMessage
    ) {
        var existing = repository.findById(baseWorkflowId);
        if (existing.isEmpty()) {
            return baseWorkflowId;
        }
        var existingVersion = existing.get().state().workflowDefinitionId().version();
        if (existingVersion.equals(newInstanceVersion)) {
            logger.warn(
                    "A workflow with id '{}' is already running at version '{}'; ignoring new start request "
                            + "triggered by event '{}'. The new start would be at the same version, so it is "
                            + "treated as a duplicate. To run two instances of the same version in parallel, "
                            + "use a different idProperty value.",
                    baseWorkflowId, existingVersion, eventMessage.type().qualifiedName()
            );
            return null;
        }
        if (Version.of(existingVersion).isGreaterThan(Version.of(newInstanceVersion))) {
            logger.warn(
                    "A workflow with id '{}' is already running at version '{}', above the requested version '{}'; "
                            + "ignoring new start request triggered by event '{}'. The running instance migrated past "
                            + "the registered version, so this start is treated as a duplicate.",
                    baseWorkflowId, existingVersion, newInstanceVersion, eventMessage.type().qualifiedName()
            );
            return null;
        }
        var disambiguated = baseWorkflowId + "#" + newInstanceVersion;
        if (repository.findById(disambiguated).isPresent()) {
            logger.warn(
                    "A workflow with id '{}' is already running (cross-version disambiguated from '{}' at version "
                            + "'{}'); ignoring new start request triggered by event '{}'.",
                    disambiguated, baseWorkflowId, newInstanceVersion, eventMessage.type().qualifiedName()
            );
            return null;
        }
        logger.info(
                "Starting a parallel workflow at version '{}' alongside the existing instance '{}' at version '{}'. "
                        + "Disambiguated workflow id: '{}'.",
                newInstanceVersion, baseWorkflowId, existingVersion, disambiguated
        );
        return disambiguated;
    }
}
