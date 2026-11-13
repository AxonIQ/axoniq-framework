/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.HashSet;
import java.util.Set;

/**
 * Event sourced collection of running workflow IDs, loaded by {@link WorkflowStore}.
 * <p>
 * The {@link Event#version() version} is the version of the snapshot format, see {@link #getWorkflowIds()}.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
@Event(version = "1.0.0")
@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE
)
public class EventSourcedRunningWorkflows implements RunningWorkflows {

    /**
     * Singleton identifier of the running-workflows entity.
     */
    public static final String ENTITY_ID = "__running-workflow";

    private final Set<String> workflowIds = new HashSet<>();

    /**
     * Returns the criteria selecting workflow lifecycle events introduced by ADR-007.
     *
     * @return event criteria for workflow lifecycle reconstruction
     */
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
    public void evolve(Metadata metadata) {
        MetadataUtils.getWorkflowStatus(metadata)
                     .ifPresent(status -> applyLifecycle(MetadataUtils.getWorkflowId(metadata), status));
    }

    /**
     * Returns the identifiers of the running workflows, for a converter writing this collection as a snapshot.
     *
     * @return the identifiers of the running workflows
     */
    @JsonProperty("workflowIds")
    public Set<String> getWorkflowIds() {
        return workflowIds();
    }

    /**
     * Sets the identifiers of the running workflows, for a converter reading this collection from a snapshot.
     *
     * @param workflowIds the identifiers of the running workflows
     */
    @JsonProperty("workflowIds")
    public void setWorkflowIds(Set<String> workflowIds) {
        this.workflowIds.clear();
        this.workflowIds.addAll(workflowIds);
    }

    @Override
    public Set<String> workflowIds() {
        return Set.copyOf(workflowIds);
    }

    @Override
    public boolean contains(String workflowId) {
        return workflowIds.contains(workflowId);
    }

    private void applyLifecycle(String workflowId, WorkflowStatus status) {
        switch (status) {
            case STARTED -> workflowIds.add(workflowId);
            case COMPLETED, FAILED, TIMED_OUT, CANCELLED -> workflowIds.remove(workflowId);
            case NONE -> {
                // not a lifecycle transition
            }
        }
    }
}
