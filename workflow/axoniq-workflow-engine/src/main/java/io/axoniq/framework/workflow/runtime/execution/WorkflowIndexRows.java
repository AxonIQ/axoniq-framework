/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

/**
 * The rows of one segment in a {@link WorkflowIndex}, and the position they are correct at. Internal, because the
 * {@link WorkflowEngine} uses it to restore the workflows of a claimed segment.
 *
 * @param runningWorkflowIds the identifiers of the running workflows of the segment
 * @param wakesAt            the time each hibernated workflow wakes at, by workflow identifier
 * @param waits              the open waits of the segment
 * @param position           the position the rows are correct at, or empty when this exact segment has none
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WorkflowIndexRows(Set<String> runningWorkflowIds,
                                Map<String, Instant> wakesAt,
                                Collection<WaitAssociation> waits,
                                OptionalLong position) {

    /**
     * Creates the rows of a segment, copying the given collections.
     *
     * @param runningWorkflowIds the identifiers of the running workflows of the segment
     * @param wakesAt            the time each hibernated workflow wakes at, by workflow identifier
     * @param waits              the open waits of the segment
     * @param position           the position the rows are correct at, or empty when this exact segment has none
     */
    public WorkflowIndexRows {
        runningWorkflowIds = Set.copyOf(
                Objects.requireNonNull(runningWorkflowIds, "The running workflow ids must not be null.")
        );
        wakesAt = Map.copyOf(Objects.requireNonNull(wakesAt, "The wake times must not be null."));
        waits = List.copyOf(Objects.requireNonNull(waits, "The waits must not be null."));
        Objects.requireNonNull(position, "The position must not be null.");
    }
}
