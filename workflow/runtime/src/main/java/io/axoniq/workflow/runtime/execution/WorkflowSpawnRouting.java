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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static helpers that decide workflow-id and spawn behaviour when starting a new workflow under a
 * multi-version registry. Lives outside {@link WorkflowEngine} because the decision is pure routing
 * over the repository state.
 *
 * @author Stefan Dragisic
 * @since 1.1.0
 */
@Internal
public final class WorkflowSpawnRouting {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowSpawnRouting.class);

    private WorkflowSpawnRouting() {
    }

    /**
     * Reports whether the given configuration's {@code workflowIdProvider} derived a workflow id from the given event,
     * logging the misconfiguration that produced no id and rejecting the spawn when it did not.
     * <p>
     * A provider deriving no id (an {@code idProperty} naming a property the event does not carry, typically) is never
     * transient: every event of that type derives the same nothing. Failing the spawn would fail the work package and
     * stall the segment processing it, taking down every instance that segment owns instead of just the misconfigured
     * definition; skipping the spawn keeps the damage to that one definition, and naming the definition and the event
     * here keeps it attributable. The provider is an arbitrary function over the event that resolves property names
     * against the converted payload, so registration cannot reject it up front.
     *
     * @param baseWorkflowId        the id the provider derived, or {@code null} when it derived none.
     * @param workflowConfiguration configuration whose provider was asked.
     * @param eventMessage          event the id was to be derived from.
     * @return {@code true} when an id was derived and the spawn may proceed.
     */
    public static boolean hasDerivedWorkflowId(
            @Nullable String baseWorkflowId,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration,
            @Nonnull EventMessage eventMessage
    ) {
        if (baseWorkflowId != null) {
            return true;
        }
        logger.error(
                "The workflowIdProvider ({}) of workflow '{}' version '{}' derived no workflow id from event '{}'; "
                        + "not spawning an instance. Check that the configured idProperty names a property the event "
                        + "actually carries.",
                workflowConfiguration.workflowIdProvider().getClass().getName(),
                workflowConfiguration.workflowName(),
                workflowConfiguration.workflowVersion(),
                eventMessage.type().qualifiedName()
        );
        return false;
    }

    /**
     * Decides the workflow id to use for a new spawn given an existing execution repository:
     * <ul>
     *   <li>No existing execution at {@code baseWorkflowId} → return the base id unchanged.</li>
     *   <li>Existing execution at the <em>same</em> version → reject the spawn (returns {@code null})
     *       and warn.</li>
     *   <li>Existing execution at a <em>different</em> version → return {@code <base>#<version>} so
     *       both versions coexist in parallel.</li>
     *   <li>The disambiguated id is also already taken → reject and warn.</li>
     * </ul>
     */
    @Nullable
    public static String resolveWorkflowIdForNewSpawn(
            @Nonnull WorkflowExecutionRepository repository,
            @Nonnull String baseWorkflowId,
            @Nonnull String newSpawnVersion,
            @Nonnull EventMessage eventMessage
    ) {
        var existing = repository.findById(baseWorkflowId);
        if (existing.isEmpty()) {
            return baseWorkflowId;
        }
        var existingVersion = existing.get().state().workflowDefinitionVersion();
        if (existingVersion.equals(newSpawnVersion)) {
            logger.warn(
                    "A workflow with id '{}' is already running at version '{}'; ignoring new start request "
                            + "triggered by event '{}'. The new spawn would be at the same version, so it is "
                            + "treated as a duplicate. To run two instances of the same version in parallel, "
                            + "use a different idProperty value.",
                    baseWorkflowId, existingVersion, eventMessage.type().qualifiedName()
            );
            return null;
        }
        var disambiguated = baseWorkflowId + "#" + newSpawnVersion;
        if (repository.findById(disambiguated).isPresent()) {
            logger.warn(
                    "A workflow with id '{}' is already running (cross-version disambiguated from '{}' at version "
                            + "'{}'); ignoring new start request triggered by event '{}'.",
                    disambiguated, baseWorkflowId, newSpawnVersion, eventMessage.type().qualifiedName()
            );
            return null;
        }
        logger.info(
                "Spawning a parallel workflow at version '{}' alongside the existing instance '{}' at version '{}'. "
                        + "Disambiguated workflow id: '{}'.",
                newSpawnVersion, baseWorkflowId, existingVersion, disambiguated
        );
        return disambiguated;
    }
}
