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

import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import io.axoniq.framework.messaging.transformation.FromMatcher;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Immutable lookup of the {@link EventTransformation} applying to an event.
 * <p>
 * An exact identity ({@code from}-by-{@link MessageType}) match always wins over a predicate match: predicates are
 * consulted only when no exact transformation matches, and among predicates the first registered match wins. Exact
 * matches do not depend on registration order; two transformations matching the same exact source are rejected when
 * the index is built. Exact-{@code from} transformations are bucketed by {@link QualifiedName} for constant-time
 * lookup; predicate-{@code from} transformations are scanned in registration order.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class TransformationIndex {

    private final Map<QualifiedName, List<EventTransformation>> exactTransformationsByName;
    private final List<EventTransformation> predicateTransformations;

    private TransformationIndex(Map<QualifiedName, List<EventTransformation>> exactTransformationsByName,
                                List<EventTransformation> predicateTransformations) {
        this.exactTransformationsByName = copyImmutable(exactTransformationsByName);
        this.predicateTransformations = List.copyOf(predicateTransformations);
    }

    /**
     * Builds an index from the transformations in registration order. Order is retained only for predicate matching;
     * exact matches are resolved by identity and are order-independent.
     *
     * @param registrationOrder the registered transformations, oldest first
     * @return the immutable index, never {@code null}
     * @throws ChainConfigurationException if two transformations match the same exact source identity
     */
    static TransformationIndex of(List<EventTransformation> registrationOrder) {
        Map<QualifiedName, List<EventTransformation>> exactBuckets = new HashMap<>();
        List<EventTransformation> predicateList = new ArrayList<>();
        Map<MessageType, EventTransformation> claimedSources = new HashMap<>();
        for (EventTransformation transformation : registrationOrder) {
            switch (transformation.matcher()) {
                case FromMatcher.Exact(MessageType source) ->
                        indexExact(transformation, source, exactBuckets, claimedSources);
                case FromMatcher.PredicateBased ignored -> predicateList.add(transformation);
            }
        }
        return new TransformationIndex(exactBuckets, predicateList);
    }

    /**
     * Buckets an exact transformation under its source's qualified name, after rejecting a source already claimed by
     * an earlier transformation.
     * <p>
     * Each exact transformation carries a single source, so it lands under exactly one qualified name. That is what
     * lets {@link #count()} and {@link #transformations()} treat the buckets as a partition without double-counting.
     */
    private static void indexExact(EventTransformation transformation,
                                   MessageType source,
                                   Map<QualifiedName, List<EventTransformation>> exactBuckets,
                                   Map<MessageType, EventTransformation> claimedSources) {
        EventTransformation previous = claimedSources.putIfAbsent(source, transformation);
        if (previous != null) {
            throw duplicateSource(source, previous, transformation);
        }
        exactBuckets.computeIfAbsent(source.qualifiedName(), ignored -> new ArrayList<>()).add(transformation);
    }

    private static ChainConfigurationException duplicateSource(MessageType source,
                                                               EventTransformation previous,
                                                               EventTransformation current) {
        return new ChainConfigurationException("""
                Two transformations match the same source %s; an exact source may be claimed only once. \
                Conflicting transformations: %s and %s.""".formatted(source, previous, current));
    }

    /**
     * Returns the transformation matching {@code eventType}, or {@code null} when none matches. An exact match wins
     * over a predicate match; among predicates the first registered match wins.
     *
     * @param eventType the type of the event being transformed
     * @return the matching transformation, or {@code null} if none matches
     */
    @Nullable
    EventTransformation findMatch(MessageType eventType) {
        EventTransformation exactMatch = findExactMatch(eventType);
        return exactMatch != null ? exactMatch : findFirstPredicateMatch(eventType);
    }

    private @Nullable EventTransformation findExactMatch(MessageType eventType) {
        List<EventTransformation> bucket = exactTransformationsByName.get(eventType.qualifiedName());
        if (bucket == null) {
            return null;
        }
        // At most one candidate can match: every exact source is claimed by a single transformation (duplicates
        // are rejected when the index is built), so the first match is necessarily the only match.
        for (EventTransformation candidate : bucket) {
            if (candidate.matcher().matches(eventType)) {
                return candidate;
            }
        }
        return null;
    }

    private @Nullable EventTransformation findFirstPredicateMatch(MessageType eventType) {
        for (EventTransformation candidate : predicateTransformations) {
            if (candidate.matcher().matches(eventType)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Returns all registered transformations.
     *
     * @return a stream of all registered transformations
     */
    Stream<EventTransformation> transformations() {
        return Stream.concat(exactTransformationsByName.values().stream().flatMap(List::stream),
                             predicateTransformations.stream());
    }

    /**
     * Counts every registered transformation, across both the exact buckets and the predicate list.
     *
     * @return the number of registered transformations
     */
    int count() {
        return predicateTransformations.size()
                + exactTransformationsByName.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Renders, for framework diagnostics, the exact-{@code from} transformations bucketed by qualified name.
     *
     * @return exact-{@code from} buckets rendered as {@code qualifiedName -> [transformation-toString, ...]}
     */
    Map<String, List<String>> exactTransformationsDescription() {
        Map<String, List<String>> rendered = HashMap.newHashMap(exactTransformationsByName.size());
        exactTransformationsByName.forEach((qualifiedName, bucket) ->
                rendered.put(qualifiedName.name(), bucket.stream().map(Object::toString).toList()));
        return rendered;
    }

    /**
     * Renders, for framework diagnostics, the predicate-{@code from} transformations in registration order.
     *
     * @return predicate-{@code from} transformations rendered by their {@code toString()}
     */
    List<String> predicateTransformationsDescription() {
        return predicateTransformations.stream().map(Object::toString).toList();
    }

    private static Map<QualifiedName, List<EventTransformation>> copyImmutable(
            Map<QualifiedName, List<EventTransformation>> source) {
        Map<QualifiedName, List<EventTransformation>> copy = HashMap.newHashMap(source.size());
        source.forEach((key, bucket) -> copy.put(key, List.copyOf(bucket)));
        return Map.copyOf(copy);
    }
}
