/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
 * exact equality against a fixed {@link MessageType} ({@link Exact}) or a
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
     * Exact-source matcher: matches an event whose {@link MessageType} equals a fixed identity.
     *
     * @param source the exact identity to match
     */
    record Exact(MessageType source) implements FromMatcher {

        public Exact {
            requireNonNull(source, "source may not be null");
        }

        @Override
        public boolean matches(MessageType candidate) {
            return source.equals(candidate);
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
