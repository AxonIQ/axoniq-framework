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
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Immutable, last-match-wins lookup of the {@link EventTransformation} applying to an event. Exact-{@code
 * from} transformations are bucketed by {@link QualifiedName} for constant-time lookup; predicate-{@code from}
 * transformations are scanned in registration order. A later registration wins a tie.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class TransformationIndex {

    /**
     * Exact-{@code from} transformations bucketed by {@link QualifiedName}, each bucket in registration order. The
     * per-transformation {@code sequence} preserves the registration order across this map and
     * {@link #predicateTransformations}.
     */
    private final Map<QualifiedName, List<RegisteredTransformation>> exactTransformationsByName;

    /** Predicate-{@code from} transformations in registration order. */
    private final List<RegisteredTransformation> predicateTransformations;

    private TransformationIndex(Map<QualifiedName, List<RegisteredTransformation>> exactTransformationsByName,
                                List<RegisteredTransformation> predicateTransformations) {
        this.exactTransformationsByName = copyImmutable(exactTransformationsByName);
        this.predicateTransformations = List.copyOf(predicateTransformations);
    }

    /**
     * Builds an index from the transformations in registration order; a later registration wins a tie in
     * {@link #findLastMatch(MessageType)}.
     *
     * @param registrationOrder the registered transformations, oldest first
     * @return the immutable index, never {@code null}
     */
    static TransformationIndex of(List<EventTransformation> registrationOrder) {
        Map<QualifiedName, List<RegisteredTransformation>> exactBuckets = new HashMap<>();
        List<RegisteredTransformation> predicateList = new ArrayList<>();
        long sequence = 0;
        for (EventTransformation transformation : registrationOrder) {
            RegisteredTransformation entry = new RegisteredTransformation(sequence++, transformation);
            switch (transformation.matcher()) {
                case FromMatcher.Exact(MessageType source) -> exactBuckets
                        .computeIfAbsent(source.qualifiedName(), ignored -> new ArrayList<>())
                        .add(entry);
                case FromMatcher.PredicateBased ignored -> predicateList.add(entry);
            }
        }
        return new TransformationIndex(exactBuckets, predicateList);
    }

    /**
     * Returns the latest-registered transformation matching {@code eventType}, or {@code null} when none matches.
     * Scans only the relevant exact bucket and the predicate list, picking whichever has the higher overall
     * registration order.
     *
     * @param eventType the type of the event being transformed
     * @return the matching transformation, or {@code null} if none matches
     */
    @Nullable
    EventTransformation findLastMatch(MessageType eventType) {
        List<RegisteredTransformation> exactBucket = exactTransformationsByName.get(eventType.qualifiedName());
        if (exactBucket == null && predicateTransformations.isEmpty()) {
            return null;
        }
        RegisteredTransformation lastExact = exactBucket == null ? null : findLastMatchIn(exactBucket, eventType);
        RegisteredTransformation lastPredicate = findLastMatchIn(predicateTransformations, eventType);
        return pickLaterRegistration(lastExact, lastPredicate);
    }

    private static @Nullable RegisteredTransformation findLastMatchIn(List<RegisteredTransformation> bucket,
                                                                      MessageType eventType) {
        for (int index = bucket.size() - 1; index >= 0; index--) {
            RegisteredTransformation candidate = bucket.get(index);
            if (candidate.transformation().matcher().matches(eventType)) {
                return candidate;
            }
        }
        return null;
    }

    private static @Nullable EventTransformation pickLaterRegistration(
            @Nullable RegisteredTransformation exactCandidate,
            @Nullable RegisteredTransformation predicateCandidate) {
        if (exactCandidate == null) {
            return predicateCandidate == null ? null : predicateCandidate.transformation();
        }
        if (predicateCandidate == null) {
            return exactCandidate.transformation();
        }
        return exactCandidate.sequence() > predicateCandidate.sequence()
                ? exactCandidate.transformation()
                : predicateCandidate.transformation();
    }

    /**
     * Returns all registered transformations.
     *
     * @return a stream of all registered transformations
     */
    Stream<EventTransformation> transformations() {
        return Stream.concat(exactTransformationsByName.values().stream().flatMap(List::stream),
                             predicateTransformations.stream())
                     .map(RegisteredTransformation::transformation);
    }

    /**
     * @return the number of registered transformations
     */
    int count() {
        return predicateTransformations.size()
                + exactTransformationsByName.values().stream().mapToInt(List::size).sum();
    }

    /**
     * @return exact-{@code from} buckets rendered as {@code qualifiedName -> [transformation-toString, ...]}
     */
    Map<String, List<String>> exactTransformationsDescription() {
        Map<String, List<String>> rendered = HashMap.newHashMap(exactTransformationsByName.size());
        exactTransformationsByName.forEach((qualifiedName, bucket) ->
                rendered.put(qualifiedName.name(),
                             bucket.stream().map(entry -> entry.transformation().toString()).toList()));
        return rendered;
    }

    /**
     * @return predicate-{@code from} transformations rendered by their {@code toString()}
     */
    List<String> predicateTransformationsDescription() {
        return predicateTransformations.stream().map(entry -> entry.transformation().toString()).toList();
    }

    private static Map<QualifiedName, List<RegisteredTransformation>> copyImmutable(
            Map<QualifiedName, List<RegisteredTransformation>> source) {
        Map<QualifiedName, List<RegisteredTransformation>> copy = HashMap.newHashMap(source.size());
        source.forEach((key, bucket) -> copy.put(key, List.copyOf(bucket)));
        return Map.copyOf(copy);
    }

    /**
     * A registered transformation paired with its registration {@code sequence}, so the latest registration wins a
     * tie across the exact buckets and the predicate list without keeping a parallel flat list.
     */
    private record RegisteredTransformation(long sequence, EventTransformation transformation) {

    }
}
