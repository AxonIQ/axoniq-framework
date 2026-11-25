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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The rows of one segment in a {@link WorkflowIndex}: its running workflows, the hibernated ones among them, and its
 * open waits.
 * <p>
 * Internal, because it is part of how the {@link WorkflowEngine} restores the workflows of a claimed segment.
 *
 * @param runningWorkflowIds the identifiers of the running workflows of the segment
 * @param wakesAt            the time each hibernated workflow wakes at, by workflow identifier. A workflow without an
 *                           entry is not known to hibernate
 * @param waits              the open waits of the segment
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WorkflowIndexRows(Set<String> runningWorkflowIds,
                                Map<String, Instant> wakesAt,
                                Collection<WaitAssociation> waits) {

    /**
     * No rows, for a segment without running workflows.
     */
    public static final WorkflowIndexRows NONE = new WorkflowIndexRows(Set.of(), Map.of(), List.of());

    /**
     * Creates the rows of a segment, copying the given collections.
     *
     * @param runningWorkflowIds the identifiers of the running workflows of the segment
     * @param wakesAt            the time each hibernated workflow wakes at, by workflow identifier
     * @param waits              the open waits of the segment
     */
    public WorkflowIndexRows {
        runningWorkflowIds = new HashSet<>(
                Objects.requireNonNull(runningWorkflowIds, "The running workflow ids must not be null.")
        );
        wakesAt = new HashMap<>(Objects.requireNonNull(wakesAt, "The wake times must not be null."));
        waits = new ArrayList<>(Objects.requireNonNull(waits, "The waits must not be null."));
    }
}
