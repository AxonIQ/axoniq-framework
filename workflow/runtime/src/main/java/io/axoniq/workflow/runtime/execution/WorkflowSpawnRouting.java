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
 * @since 0.2.0
 */
@Internal
public final class WorkflowSpawnRouting {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowSpawnRouting.class);

    private WorkflowSpawnRouting() {
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
        var existingVersion = existing.get().state().workflowDefinitionId().version();
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
