/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

/**
 * Constant holder for workflow event tags.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowEventTags {

    /**
     * Value for workflow lifecycle events.
     */
    public static final String TAG_VALUE_EVENT_TYPE_LIFECYCLE = "lifecycle";
    /**
     * A tag value for a waitForEvent step.
     */
    public static final String TAG_VALUE_EVENT_TYPE_WAIT_STEP = "waitForStep";
    /**
     * Tag key for workflow id.
     */
    public static final String TAG_WORKFLOW_ID = "workflowId";
    /**
     * Tag key for workflow lifecycle.
     */
    public static final String TAG_WORKFLOW_EVENT_TYPE = "workflowEvent";

    private WorkflowEventTags() {
        // avoid instantiation
    }
}
