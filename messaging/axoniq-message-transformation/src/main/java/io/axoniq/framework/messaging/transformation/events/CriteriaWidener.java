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

package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.FromMatcher;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.EventCriterion;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Immutable, thread-safe widener of read {@link EventCriteria}. Broadens a query so it also returns events stored
 * under types that the chain's transformations convert into the queried types.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class CriteriaWidener {

    /**
     * Maps a target type name to the source type names that transform into it.
     */
    private final Map<QualifiedName, Set<QualifiedName>> sourcesByTarget;

    /**
     * Target type names whose read type filter must be dropped because their source types cannot be enumerated.
     */
    private final Set<QualifiedName> droppingTargets;

    /**
     * Whether any criterion can be widened.
     */
    private final boolean active;

    private CriteriaWidener(Map<QualifiedName, Set<QualifiedName>> sourcesByTarget,
                            Set<QualifiedName> droppingTargets) {
        this.sourcesByTarget = copyImmutable(sourcesByTarget);
        this.droppingTargets = Set.copyOf(droppingTargets);
        this.active = !this.sourcesByTarget.isEmpty()
                || !this.droppingTargets.isEmpty();
    }

    /**
     * Builds a widener from a chain's transformations.
     *
     * @param transformations the transformations registered with the chain, cannot be {@code null}
     * @return the immutable widener, never {@code null}
     */
    static CriteriaWidener of(Stream<EventTransformation> transformations) {
        Map<QualifiedName, Set<QualifiedName>> sourcesByTarget = new HashMap<>();
        Set<QualifiedName> droppingTargets = new HashSet<>();
        transformations.forEach(transformation -> index(transformation, sourcesByTarget, droppingTargets));
        return new CriteriaWidener(sourcesByTarget, droppingTargets);
    }

    /**
     * Widens the given criteria so a type-filtering read still returns every event this chain can
     * transform into one of the queried types. Returns the same instance when nothing can be
     * widened or no criterion is affected.
     *
     * @param criteria the read-time criteria to widen
     * @return the widened criteria, or the same instance when nothing is broadened
     */
    EventCriteria widen(EventCriteria criteria) {
        if (!active) {
            return criteria;
        }
        return criteria.mapEachCriterion(this::widenCriterion);
    }

    /**
     * Widens a single criterion, expanding its queried types with their reachable source types.
     */
    private EventCriteria widenCriterion(EventCriterion criterion) {
        Set<QualifiedName> queriedTypes = criterion.types();
        if (queriedTypes.isEmpty()) {
            return criterion;
        }
        Reachability reachability = reachableFrom(queriedTypes);
        Set<Tag> tags = criterion.tags();
        if (reachability.dropTypeFilter()) {
            return tags.isEmpty() ? EventCriteria.havingAnyTag() : EventCriteria.havingTags(tags);
        }
        if (reachability.types().equals(queriedTypes)) {
            return criterion;
        }
        Set<QualifiedName> widenedTypes = Set.copyOf(reachability.types());
        return tags.isEmpty()
                ? EventCriteria.havingAnyTag().andBeingOneOfTypes(widenedTypes)
                : EventCriteria.havingTags(tags).andBeingOneOfTypes(widenedTypes);
    }

    /**
     * Returns the backward closure of the queried types over the widening graph, and whether any
     * reachable type is produced by a type-filter-dropping transformation.
     */
    private Reachability reachableFrom(Set<QualifiedName> queriedTypes) {
        Set<QualifiedName> reachable = new HashSet<>();
        Deque<QualifiedName> pending = new ArrayDeque<>(queriedTypes);
        boolean dropTypeFilter = false;
        while (!pending.isEmpty()) {
            QualifiedName name = pending.poll();
            if (!reachable.add(name)) {
                continue;
            }
            if (droppingTargets.contains(name)) {
                dropTypeFilter = true;
            }
            Set<QualifiedName> sources = sourcesByTarget.get(name);
            if (sources != null) {
                pending.addAll(sources);
            }
        }
        return new Reachability(reachable, dropTypeFilter);
    }

    private static void index(EventTransformation transformation,
                              Map<QualifiedName, Set<QualifiedName>> sourcesByTarget,
                              Set<QualifiedName> droppingTargets) {
        // A mapping and a rename both declare one 'to' type that contributes to one widening edge
        // A split contributes one edge per declared 'to' type
        // A drop declares no 'to'.
        switch (transformation) {
            case MappingEventTransformation<?, ?> mapping ->
                    indexEdges(mapping.matcher(), mapping.toType().qualifiedName(), sourcesByTarget, droppingTargets);
            case RenameEventTransformation rename ->
                    indexEdges(rename.matcher(), rename.toType().qualifiedName(), sourcesByTarget, droppingTargets);
            case SplitEventTransformation<?> split ->
                    split.declaredToTypes().forEach(target ->
                            indexEdges(split.matcher(), target, sourcesByTarget, droppingTargets));
            case DropEventTransformation ignored -> {
                // A drop produces no event, so it adds no widening edge: a read never needs to fetch a dropped type.
            }
        }
    }

    private static void indexEdges(FromMatcher matcher,
                                   QualifiedName target,
                                   Map<QualifiedName, Set<QualifiedName>> sourcesByTarget,
                                   Set<QualifiedName> droppingTargets) {
        switch (matcher) {
            case FromMatcher.Exact(MessageType source) ->
                    addSource(sourcesByTarget, target, source.qualifiedName());
            case FromMatcher.PredicateBased(Predicate<MessageType> ignored, Set<QualifiedName> declaredFromTypes) -> {
                if (declaredFromTypes.isEmpty()) {
                    droppingTargets.add(target);
                } else {
                    declaredFromTypes.forEach(source -> addSource(sourcesByTarget, target, source));
                }
            }
        }
    }

    private static void addSource(Map<QualifiedName, Set<QualifiedName>> sourcesByTarget,
                                  QualifiedName target,
                                  QualifiedName source) {
        if (!source.equals(target)) {
            sourcesByTarget.computeIfAbsent(target, ignored -> new HashSet<>()).add(source);
        }
    }

    /**
     * Renders, for framework diagnostics, the widening edges from each target type to the source types that
     * transform into it.
     *
     * @return the widening closure rendered as {@code target -> [source qualified name, ...]}
     */
    Map<String, List<String>> graphDescription() {
        Map<String, List<String>> rendered = HashMap.newHashMap(sourcesByTarget.size());
        sourcesByTarget.forEach((target, sources) ->
                rendered.put(target.name(), sources.stream().map(QualifiedName::name).sorted().toList()));
        return rendered;
    }

    /**
     * Renders, for framework diagnostics, the target types whose read-time type filter is dropped because their
     * source types cannot be enumerated.
     *
     * @return the type-filter-dropping target names, rendered by qualified name
     */
    List<String> typeFilterDroppingDescription() {
        return droppingTargets.stream().map(QualifiedName::name).sorted().toList();
    }

    private static Map<QualifiedName, Set<QualifiedName>> copyImmutable(
            Map<QualifiedName, Set<QualifiedName>> source) {
        Map<QualifiedName, Set<QualifiedName>> copy = HashMap.newHashMap(source.size());
        source.forEach((target, sources) -> copy.put(target, Set.copyOf(sources)));
        return Map.copyOf(copy);
    }

    /**
     * The result of walking the backward closure: the reachable source types, and whether the
     * type filter must be dropped because a type-filter-dropping transformation was encountered.
     */
    private record Reachability(Set<QualifiedName> types, boolean dropTypeFilter) {

    }
}
