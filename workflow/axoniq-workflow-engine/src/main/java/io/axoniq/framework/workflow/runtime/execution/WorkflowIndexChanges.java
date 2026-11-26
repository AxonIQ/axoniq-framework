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
 * The changes a {@link WorkflowIndex} stores in one call, in order, and the position of the token they are stored
 * with. Internal, because the {@link WorkflowEngine} uses it to keep the index current.
 *
 * @param changes  the changes, in the order to store them
 * @param position the position of the token of the segment, for the workflow manager, or {@code null} to keep it
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WorkflowIndexChanges(List<WorkflowIndexChange> changes, @Nullable SegmentPosition position) {

    /**
     * Creates the changes of one store, copying the given list.
     *
     * @param changes  the changes, in the order to store them
     * @param position the position of the token of the segment, for the workflow manager, or {@code null} to keep it
     */
    public WorkflowIndexChanges {
        changes = List.copyOf(Objects.requireNonNull(changes, "The changes must not be null."));
    }

    /**
     * Creates the changes of one store that keeps the position.
     *
     * @param changes the changes, in the order to store them
     */
    public WorkflowIndexChanges(List<WorkflowIndexChange> changes) {
        this(changes, null);
    }

    /**
     * The position of the last token stored for one segment. The workflow manager reads the events after the lowest
     * position of a module; a segment claim ignores it.
     *
     * @param range    the range of segment keys of the segment
     * @param position the global position in the event store of the token
     */
    public record SegmentPosition(SegmentKeyRange range, long position) {

        /**
         * Creates the position of the segment with the given range.
         *
         * @param range    the range of segment keys of the segment
         * @param position the global position in the event store of the token
         */
        public SegmentPosition {
            Objects.requireNonNull(range, "The segment key range must not be null.");
        }
    }
}
