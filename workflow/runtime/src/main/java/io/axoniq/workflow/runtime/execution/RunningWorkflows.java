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

import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import io.axoniq.workflow.runtime.util.WorkflowEventTagResolver;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.HashSet;
import java.util.Set;

/**
 * Event-sourced state holding the workflow identifiers that are currently running.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class RunningWorkflows {

    /**
     * Singleton identifier of the running-workflows entity.
     */
    public static final String ENTITY_ID = "__running-workflows";

    private final Set<String> workflowIds = new HashSet<>();

    /**
     * Criteria selecting the workflow lifecycle events introduced by ADR-007.
     *
     * @return event criteria for workflow lifecycle reconstruction
     */
    @Nonnull
    public static EventCriteria workflowLifecycleEvents() {
        return EventCriteria.havingTags(Tag.of(
                                                WorkflowEventTagResolver.TAG_WORKFLOW_EVENT_TYPE,
                                                WorkflowEventTagResolver.TAG_VALUE_EVENT_TYPE_LIFECYCLE
                                        )
        );
    }

    /**
     * Evolves the state from workflow lifecycle metadata.
     *
     * @param metadata lifecycle event metadata
     */
    public void evolve(@Nonnull Metadata metadata) {
        MetadataUtils.getWorkflowStatus(metadata)
                     .ifPresent(status -> applyLifecycle(MetadataUtils.getWorkflowId(metadata), status));
    }

    /**
     * Returns the current set of running workflow identifiers.
     *
     * @return immutable set of workflow identifiers
     */
    @Nonnull
    public Set<String> workflowIds() {
        return Set.copyOf(workflowIds);
    }

    /**
     * Checks whether the given workflow identifier is currently running.
     *
     * @param workflowId workflow identifier to inspect
     * @return {@code true} if the workflow is running
     */
    public boolean contains(@Nonnull String workflowId) {
        return workflowIds.contains(workflowId);
    }

    private void applyLifecycle(@Nonnull String workflowId, @Nonnull WorkflowStatus status) {
        switch (status) {
            case STARTED -> workflowIds.add(workflowId);
            case COMPLETED, FAILED, TIMED_OUT, CANCELLED -> workflowIds.remove(workflowId);
            case NONE -> {
                // not a lifecycle transition
            }
        }
    }
}
