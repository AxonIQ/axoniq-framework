/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The changes a {@link WorkflowIndex} stores in one call, in the order they happened.
 * <p>
 * Internal, because it is part of how the {@link WorkflowEngine} keeps a {@link WorkflowIndex} current.
 *
 * @param changes  the changes, in the order to store them
 * @param complete whether the rows of the module are complete once the changes are stored, so a claim may read them
 *                 from then on. Only the changes of a claim that sourced every running workflow and every open wait
 *                 complete a module
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WorkflowIndexChanges(List<WorkflowIndexChange> changes, boolean complete) {

    /**
     * Creates the changes of one store, copying the given list.
     *
     * @param changes  the changes, in the order to store them
     * @param complete whether the rows of the module are complete once the changes are stored
     */
    public WorkflowIndexChanges {
        changes = new ArrayList<>(Objects.requireNonNull(changes, "The changes must not be null."));
    }
}
