/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.history.api;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;

/**
 * Represents a workflow history of a passed execution.
 *
 * @param workflowId id of the historic execution.
 * @param state      resulting (final) state of the workflow.
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public record WorkflowHistory(
        String workflowId,
        WorkflowState state
) {

}
