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

/**
 * Constant holder for workflow event tags.
 *
 * @author Simon Zambrovski
 * @since 0.1.0
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
