/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * The changes a {@link WorkflowIndex} stores in one call, in the order they happened, and the position the rows of a
 * segment are correct at once they are stored.
 * <p>
 * Internal, because it is part of how the {@link WorkflowEngine} keeps a {@link WorkflowIndex} current.
 *
 * @param changes  the changes, in the order to store them
 * @param position the position the rows of a segment are correct at once the changes are stored, or {@code null} to
 *                 leave every position as it is
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WorkflowIndexChanges(List<WorkflowIndexChange> changes, @Nullable SegmentPosition position) {

    /**
     * Creates the changes of one store, copying the given list.
     *
     * @param changes  the changes, in the order to store them
     * @param position the position the rows of a segment are correct at once the changes are stored, or {@code null}
     *                 to leave every position as it is
     */
    public WorkflowIndexChanges {
        changes = List.copyOf(Objects.requireNonNull(changes, "The changes must not be null."));
    }

    /**
     * The position the rows of one segment are correct at: every lifecycle and wait event of the segment before it is
     * applied to them. A claim of that segment reads the events from there.
     *
     * @param range    the range of segment keys of the segment
     * @param position the global position in the event store the rows are correct at
     */
    public record SegmentPosition(SegmentKeyRange range, long position) {

        /**
         * Creates the position of the segment with the given range.
         *
         * @param range    the range of segment keys of the segment
         * @param position the global position in the event store the rows are correct at
         */
        public SegmentPosition {
            Objects.requireNonNull(range, "The segment key range must not be null.");
        }
    }
}
