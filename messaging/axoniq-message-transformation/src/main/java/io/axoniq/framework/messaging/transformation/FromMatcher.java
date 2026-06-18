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

package io.axoniq.framework.messaging.transformation;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Set;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Sealed strategy for matching the {@code from} side of a transformation. Either
 * exact equality against one of a fixed set of {@link MessageType}s ({@link Exact}) or a
 * user-supplied {@link Predicate} ({@link PredicateBased}).
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
public sealed interface FromMatcher permits FromMatcher.Exact, FromMatcher.PredicateBased {

    /**
     * Answers whether the given {@code candidate} type satisfies this matcher.
     *
     * @param candidate the message's {@link MessageType}
     * @return {@code true} when the candidate matches
     */
    boolean matches(MessageType candidate);

    /**
     * Exact-source matcher: matches an event whose {@link MessageType} equals one of a fixed set of identities.
     *
     * @param sources the exact identities to match; at least one is required
     */
    record Exact(Set<MessageType> sources) implements FromMatcher {

        public Exact {
            sources = Set.copyOf(requireNonNull(sources, "sources may not be null"));
            if (sources.isEmpty()) {
                throw new IllegalArgumentException("An exact matcher requires at least one source.");
            }
        }

        /**
         * Creates a matcher for a single exact identity.
         *
         * @param source the exact identity to match
         * @return the matcher, never {@code null}
         */
        public static Exact of(MessageType source) {
            return new Exact(Set.of(requireNonNull(source, "source may not be null")));
        }

        @Override
        public boolean matches(MessageType candidate) {
            return sources.contains(candidate);
        }
    }

    /**
     * Predicate-source matcher: matches any {@link MessageType} the predicate accepts. When
     * {@code declaredFromTypes} is non-empty it acts as a pre-filter: only events whose
     * {@link MessageType#qualifiedName()} is one of those names ever reach the predicate. An empty
     * set evaluates the predicate against every candidate.
     *
     * @param predicate         the source predicate
     * @param declaredFromTypes the qualified names the predicate is restricted to; empty means every candidate
     */
    record PredicateBased(Predicate<MessageType> predicate,
                          Set<QualifiedName> declaredFromTypes) implements FromMatcher {

        public PredicateBased {
            requireNonNull(predicate, "predicate may not be null");
            declaredFromTypes = Set.copyOf(requireNonNull(declaredFromTypes, "declaredFromTypes may not be null"));
        }

        @Override
        public boolean matches(MessageType candidate) {
            if (!declaredFromTypes.isEmpty() && !declaredFromTypes.contains(candidate.qualifiedName())) {
                return false;
            }
            return predicate.test(candidate);
        }
    }
}
