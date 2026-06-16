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

import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Sealed strategy for matching the {@code from} side of a transformation. Either
 * exact equality against a single {@link MessageType} ({@link Exact}) or a
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
     * Exact-source matcher: exact {@link MessageType} equality.
     *
     * @param source the source identity
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
     * Predicate-source matcher: matches any {@link MessageType} the predicate accepts.
     *
     * @param predicate the source predicate
     */
    record PredicateBased(Predicate<MessageType> predicate) implements FromMatcher {

        public PredicateBased {
            requireNonNull(predicate, "predicate may not be null");
        }

        @Override
        public boolean matches(MessageType candidate) {
            return predicate.test(candidate);
        }
    }
}
