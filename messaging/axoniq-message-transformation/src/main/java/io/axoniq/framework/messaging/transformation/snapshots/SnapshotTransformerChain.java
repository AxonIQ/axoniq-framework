/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * The registered {@link SnapshotTransformation snapshot transformations} of an application, used to find and lift
 * snapshots stored under a name or version an entity is no longer declared with.
 * <p>
 * When an entity is sourced, the chain resolves backward from the entity's current type to every identity a snapshot
 * may still be stored at, so they can all be looked up at once. A snapshot found at one of them is then lifted forward
 * along the declared path into the current type.
 * <p>
 * Registering this chain as a component enables snapshot transformation:
 * <pre>{@code
 * configurer.componentRegistry(registry -> registry.registerComponent(
 *         SnapshotTransformerChain.class,
 *         config -> SnapshotTransformerChain.builder()
 *                                           .register(accountPath)
 *                                           .build()
 * ));
 * }</pre>
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
public final class SnapshotTransformerChain implements DescribableComponent {

    private final List<SnapshotTransformation> transformations;
    private final Map<VersionedType, EdgeSnapshotTransformation> edgesBySource;
    private final Map<VersionedType, EdgeSnapshotTransformation> aggregateBasedEdgesBySource;
    private final Map<VersionedType, List<EdgeSnapshotTransformation>> edgesByTarget;
    private final List<RebuildSnapshotTransformation> rebuilds;
    private final Map<VersionedType, Lookup> lookups = new ConcurrentHashMap<>();

    private SnapshotTransformerChain(Builder builder) {
        this.transformations = List.copyOf(builder.transformations);
        this.edgesBySource = Map.copyOf(builder.edgesBySource);
        this.aggregateBasedEdgesBySource = Map.copyOf(builder.aggregateBasedEdgesBySource);
        this.rebuilds = List.copyOf(builder.rebuilds);

        Map<VersionedType, List<EdgeSnapshotTransformation>> byTarget = new HashMap<>();

        for (EdgeSnapshotTransformation edge : builder.edges) {
            byTarget.computeIfAbsent(edge.target(), k -> new ArrayList<>()).add(edge);
        }

        this.edgesByTarget = byTarget;
    }

    /**
     * Creates a builder to register transformations with.
     *
     * @return a new builder, never {@code null}
     */
    public static Builder builder() {
        return new Builder();
    }

    /*
     * Returns the candidates to look a snapshot of the given current type up under, in addition to the current type
     * itself, and the aggregate-based snapshot to look up when none of them has one. Candidates are ordered by their
     * distance from the current type along the declared paths, nearest first.
     */
    Lookup lookupFor(VersionedType current) {
        return lookups.computeIfAbsent(exact(current), this::resolveBackward);
    }

    /*
     * Lifts the state of a snapshot found at the given identity forward into the given target. Returns null when no
     * declared path leads from the found identity to the target, when the found identity is refused, or when a
     * mapper on the way declines.
     */
    @Nullable Object lift(VersionedType found,
                          boolean aggregateBased,
                          Object state,
                          VersionedType target,
                          Converter defaultConverter,
                          @Nullable ProcessingContext context) {
        VersionedType exactFound = exact(found);
        VersionedType exactTarget = exact(target);

        if (!aggregateBased && isRefused(exactFound)) {
            return null;
        }

        Map<VersionedType, EdgeSnapshotTransformation> bySource =
                aggregateBased ? aggregateBasedEdgesBySource : edgesBySource;
        EdgeSnapshotTransformation edge = bySource.get(exactFound);
        Object lifted = state;
        Converter converter = edge == null || edge.converter() == null ? defaultConverter : edge.converter();

        while (edge != null) {
            SnapshotMapping<?, ?> mapping = edge.mapping();

            if (mapping != null) {
                lifted = mapping.apply(lifted, converter, context);

                if (lifted == null) {
                    return null;
                }
            }

            if (edge.target().equals(exactTarget)) {
                return lifted;
            }

            edge = edgesBySource.get(edge.target());
            converter = defaultConverter;
        }

        return null;
    }

    private Lookup resolveBackward(VersionedType current) {
        Set<VersionedType> visited = new HashSet<>(Set.of(current));
        Deque<VersionedType> queue = new ArrayDeque<>(List.of(current));
        List<SnapshotCandidate> candidates = new ArrayList<>();
        Map<QualifiedName, Set<String>> aggregateBasedVersionsByName = new LinkedHashMap<>();

        while (!queue.isEmpty()) {
            for (EdgeSnapshotTransformation edge : edgesByTarget.getOrDefault(queue.removeFirst(), List.of())) {
                VersionedType source = edge.source();

                if (edge.aggregateBased()) {
                    aggregateBasedVersionsByName.computeIfAbsent(source.qualifiedName(), k -> new LinkedHashSet<>())
                                        .add(source.version());
                }
                else if (visited.add(source)) {
                    candidates.add(new SnapshotCandidate(source.qualifiedName(), Set.of(source.version())));
                    queue.addLast(source);
                }
            }
        }

        if (aggregateBasedVersionsByName.size() > 1) {
            throw new ChainConfigurationException(
                    "Snapshots of " + current + " can be lifted from aggregate-based snapshots of several payload "
                            + "types, but only one can be looked up: " + aggregateBasedVersionsByName.keySet()
            );
        }

        SnapshotCandidate aggregateBasedCandidate = aggregateBasedVersionsByName.entrySet().stream()
                                                                .findFirst()
                                                                .map(e -> new SnapshotCandidate(e.getKey(),
                                                                                                e.getValue()))
                                                                .orElse(null);

        return new Lookup(List.copyOf(candidates), aggregateBasedCandidate);
    }

    private boolean isRefused(VersionedType identity) {
        return rebuilds.stream().anyMatch(rebuild -> rebuild.sourcePredicate().test(identity));
    }

    private static VersionedType exact(VersionedType type) {
        return VersionedType.of(type.qualifiedName(), type.version());
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("transformations", transformations);
    }

    @Override
    public String toString() {
        return "SnapshotTransformerChain" + transformations;
    }

    /*
     * The result of resolving backward from a current type: the further candidates to look a snapshot up under, and
     * the aggregate-based snapshot to look up, if any.
     */
    record Lookup(List<SnapshotCandidate> candidates, @Nullable SnapshotCandidate aggregateBasedCandidate) {
    }

    /**
     * Builder registering the {@link SnapshotTransformation transformations} of a {@link SnapshotTransformerChain}.
     */
    public static final class Builder {

        private final List<SnapshotTransformation> transformations = new ArrayList<>();
        private final Set<EdgeSnapshotTransformation> edges = Collections.newSetFromMap(new LinkedHashMap<>());
        private final Map<VersionedType, EdgeSnapshotTransformation> edgesBySource = new HashMap<>();
        private final Map<VersionedType, EdgeSnapshotTransformation> aggregateBasedEdgesBySource = new HashMap<>();
        private final List<RebuildSnapshotTransformation> rebuilds = new ArrayList<>();

        private Builder() {
        }

        /**
         * Registers the given transformation. A path is registered with all of its edges.
         *
         * @param transformation the transformation to register, cannot be {@code null}
         * @return this builder, for chaining
         * @throws NullPointerException        if {@code transformation} is {@code null}
         * @throws ChainConfigurationException if an edge of the transformation lifts snapshots from an identity
         *                                     another registered edge already lifts from
         */
        public Builder register(SnapshotTransformation transformation) {
            requireNonNull(transformation, "transformation may not be null");
            transformations.add(transformation);

            switch (transformation) {
                case EdgeSnapshotTransformation last -> last.path().forEach(this::registerEdge);
                case RebuildSnapshotTransformation rebuild -> rebuilds.add(rebuild);
            }

            return this;
        }

        /**
         * Builds the chain.
         *
         * @return the chain, never {@code null}
         * @throws ChainConfigurationException if a refused identity is also lifted from, if the registered paths
         *                                     form a cycle, or if any identity can be lifted from Axon Framework 4
         *                                     snapshots of more than one payload type
         */
        public SnapshotTransformerChain build() {
            rejectRefusedSources();
            rejectCycles();

            SnapshotTransformerChain chain = new SnapshotTransformerChain(this);

            for (EdgeSnapshotTransformation edge : edges) {
                chain.lookupFor(edge.target());
            }

            return chain;
        }

        private void registerEdge(EdgeSnapshotTransformation edge) {
            if (!edges.add(edge)) {
                return;
            }

            Map<VersionedType, EdgeSnapshotTransformation> bySource =
                    edge.aggregateBased() ? aggregateBasedEdgesBySource : edgesBySource;
            EdgeSnapshotTransformation existing = bySource.putIfAbsent(edge.source(), edge);

            if (existing != null) {
                throw new ChainConfigurationException(
                        "Snapshots stored as " + edge.source() + " can only be lifted by one transformation, but are "
                                + "lifted by both [" + existing + "] and [" + edge + "]"
                );
            }
        }

        private void rejectRefusedSources() {
            for (EdgeSnapshotTransformation edge : edgesBySource.values()) {
                for (RebuildSnapshotTransformation rebuild : rebuilds) {
                    if (rebuild.sourcePredicate().test(edge.source())) {
                        throw new ChainConfigurationException(
                                "Snapshots stored as " + edge.source() + " are both refused by [" + rebuild
                                        + "] and lifted by [" + edge + "]"
                        );
                    }
                }
            }
        }

        private void rejectCycles() {
            for (VersionedType start : edgesBySource.keySet()) {
                Set<VersionedType> seen = new HashSet<>(Set.of(start));

                for (EdgeSnapshotTransformation edge = edgesBySource.get(start);
                     edge != null;
                     edge = edgesBySource.get(edge.target())) {
                    if (!seen.add(edge.target())) {
                        throw new ChainConfigurationException(
                                "The registered snapshot transformations form a cycle through " + edge.target()
                        );
                    }
                }
            }
        }
    }
}
