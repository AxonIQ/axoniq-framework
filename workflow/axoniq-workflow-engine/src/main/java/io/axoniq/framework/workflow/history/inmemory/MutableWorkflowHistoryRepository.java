/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
