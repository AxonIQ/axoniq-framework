/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

import java.util.List;
import java.util.Objects;

/**
 * The changes a {@link WorkflowIndex} stores in one call, in order. Internal, because the {@link WorkflowEngine} uses
 * it to keep the index current.
 *
 * @param changes the changes, in the order to store them
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WorkflowIndexChanges(List<WorkflowIndexChange> changes) {

    /**
     * Creates the changes of one store, copying the given list.
     *
     * @param changes the changes, in the order to store them
     */
    public WorkflowIndexChanges {
        changes = List.copyOf(Objects.requireNonNull(changes, "The changes must not be null."));
    }
}
