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
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.HashSet;
import java.util.Set;

@Internal
public class EventSourcedRunningWorkflows implements RunningWorkflows {

    /**
     * Singleton identifier of the running-workflows entity.
     */
    public static final String ENTITY_ID = "__running-workflow-ids";

    private final Set<String> workflowIds = new HashSet<>();

    /**
     * Returns the criteria selecting workflow lifecycle events introduced by ADR-007.
     *
     * @return event criteria for workflow lifecycle reconstruction
     */
    @Nonnull
    public static EventCriteria criteriaBuilder() {
        return EventCriteria.havingTags(Tag.of(
                                                WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE,
                                                WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE
                                        )
        );
    }

    /**
     * Applies workflow lifecycle metadata to this collection.
     *
     * @param metadata lifecycle event metadata
     */
    public void evolve(@Nonnull Metadata metadata) {
        MetadataUtils.getWorkflowStatus(metadata)
                     .ifPresent(status -> applyLifecycle(MetadataUtils.getWorkflowId(metadata), status));
    }

    @Override
    @Nonnull
    public Set<String> workflowIds() {
        return Set.copyOf(workflowIds);
    }

    @Override
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
