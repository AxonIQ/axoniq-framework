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
package io.axoniq.framework.workflow.runtime.api.manager;

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;

/**
 * Main component, providing the workflow manager API.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowManager {

    /**
     * Finds the single workflow instance matching a query.
     * <p>
     * The returned result evaluates the query when {@link WorkflowInstances.Single#single()} is invoked. That operation
     * completes with {@code null} when no instance matches and exceptionally with
     * {@link NonUniqueWorkflowInstanceMatchException} when more than one instance matches.
     *
     * @param query criteria used to select the workflow instance
     * @return lazy result for the single matching workflow instance
     */
    WorkflowInstances.Single findOne(WorkflowStateQuery query);

    /**
     * Finds every workflow instance matching a query.
     *
     * @param query criteria used to select workflow instances
     * @return lazy handle for the matching workflow instances
     */
    WorkflowInstances findMany(WorkflowStateQuery query);
}
