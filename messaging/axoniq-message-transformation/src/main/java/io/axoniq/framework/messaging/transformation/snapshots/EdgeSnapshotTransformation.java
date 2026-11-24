/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.VersionedType;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * One edge of a {@link SnapshotTransformation} path, lifting a snapshot stored at its {@link #source()} to its
 * {@link #target()}. An edge without a {@link #mapping()} carries the state over untouched.
 * <p>
 * Each edge links back to the edge before it on the same path, so registering the last edge of a path registers the
 * whole path. Only the first edge of a path may declare the {@link #converter()} its source is stored with, and only
 * the first edge may be an {@link #aggregateBased() aggregate-based} one, reading an aggregate-based snapshot.
 *
 * @param previous       the edge before this one on the same path, or {@code null} if this edge starts the path
 * @param source         the identity this edge lifts a snapshot from
 * @param aggregateBased whether the source is an aggregate-based snapshot
 * @param converter      the converter the source is stored with, or {@code null} for the application's current one
 * @param target         the identity this edge lifts a snapshot to
 * @param mapping        the mapping of the state, or {@code null} if the state is carried over untouched
 * @author John Hendrikx
 * @since 5.5.0
 */
record EdgeSnapshotTransformation(
        @Nullable EdgeSnapshotTransformation previous,
        VersionedType source,
        boolean aggregateBased,
        @Nullable Converter converter,
        VersionedType target,
        @Nullable SnapshotMapping<?, ?> mapping
) implements SnapshotTransformation {

    static EdgeSnapshotTransformation rename(@Nullable EdgeSnapshotTransformation previous,
                                             VersionedType source,
                                             VersionedType target) {
        return new EdgeSnapshotTransformation(previous, source, false, null, target, null);
    }

    static EdgeSnapshotTransformation map(@Nullable EdgeSnapshotTransformation previous,
                                          VersionedType source,
                                          boolean aggregateBased,
                                          @Nullable Converter converter,
                                          VersionedType target,
                                          SnapshotMapping<?, ?> mapping) {
        return new EdgeSnapshotTransformation(previous, source, aggregateBased, converter, target, mapping);
    }

    /*
     * Returns the edges of the path ending with this edge, from its first edge to this one.
     */
    List<EdgeSnapshotTransformation> path() {
        List<EdgeSnapshotTransformation> edges = new ArrayList<>();

        for (EdgeSnapshotTransformation edge = this; edge != null; edge = edge.previous()) {
            edges.addFirst(edge);
        }

        return edges;
    }

    @Override
    public String toString() {
        String kind = aggregateBased ? "aggregate-based " : "";

        return kind + source + " -> " + target + (mapping == null ? " (rename)" : "");
    }
}
