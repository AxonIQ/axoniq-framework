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
package io.axoniq.framework.workflow.history.inmemory;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import org.axonframework.common.annotation.Internal;

/**
 * Mutable history repository.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface MutableWorkflowHistoryRepository extends WorkflowHistoryRepository {

    /**
     * Stores an element in the repository.
     *
     * @param workflowHistory history element to store.
     */
    void save(WorkflowHistory workflowHistory);

    /**
     * Clears all entries.
     */
    void clear();
}
