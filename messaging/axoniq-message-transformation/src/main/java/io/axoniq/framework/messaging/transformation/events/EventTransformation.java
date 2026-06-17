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
import io.axoniq.framework.messaging.transformation.MessageTransformation;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * An event-specific {@link MessageTransformation}.
 * <p>
 * Construct instances with the static {@link #from(MessageType)} / {@link #from(Predicate)} entry points.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public sealed interface EventTransformation extends MessageTransformation<EventMessage>
        permits MappingEventTransformation {

    /**
     * Begin a 1:1 transformation matching the given {@code from} identity by exact equality. Continue
     * with {@code to(...)} then {@code transform(...)}.
     *
     * @param source the {@code from} identity
     * @return a builder awaiting {@code to(...)}
     */
    static ToStep from(MessageType source) {
        requireNonNull(source, "source may not be null");
        return new ToStep(new FromMatcher.Exact(source));
    }

    /**
     * Begin a 1:1 transformation matching any {@link MessageType} for which the supplied predicate returns
     * {@code true}. Optionally restrict the match to specific {@code from} type names via
     * {@link PredicateFromStep#declaringFromTypes(QualifiedName...)}, then continue with {@code to(...)}.
     *
     * @param sourcePredicate the matcher
     * @return a builder optionally restricting the match before awaiting {@code to(...)}
     */
    static PredicateFromStep from(Predicate<MessageType> sourcePredicate) {
        requireNonNull(sourcePredicate, "sourcePredicate may not be null");
        return new PredicateFromStep(sourcePredicate);
    }

    /**
     * The {@code from}-side matcher selecting the events this transformation applies to. The chain's lookup uses
     * this; the declared {@code to} identity, if any, is variant-specific and not part of this contract.
     *
     * @return the {@code from}-side matcher
     */
    @Internal
    FromMatcher matcher();

    /**
     * Continuation of {@link #from(Predicate)}; optionally restricts the predicate match to a set of declared
     * {@code from} type names before {@code to(...)} is supplied.
     */
    final class PredicateFromStep {

        private final Predicate<MessageType> sourcePredicate;

        private PredicateFromStep(Predicate<MessageType> sourcePredicate) {
            this.sourcePredicate = sourcePredicate;
        }

        /**
         * Restrict the predicate match to events whose {@link QualifiedName} is one of the given types. The
         * predicate is then evaluated only against those types, never against any other event.
         *
         * @param declaredFromTypes the {@code from} type names to match; at least one is required
         * @return a builder awaiting {@code to(...)}
         * @throws IllegalArgumentException if {@code declaredFromTypes} is empty
         */
        public ToStep declaringFromTypes(QualifiedName... declaredFromTypes) {
            requireNonNull(declaredFromTypes, "declaredFromTypes may not be null");
            Set<QualifiedName> declared = new LinkedHashSet<>(declaredFromTypes.length);
            for (QualifiedName declaredType : declaredFromTypes) {
                declared.add(requireNonNull(declaredType, "declaredFromTypes element may not be null"));
            }
            if (declared.isEmpty()) {
                throw new IllegalArgumentException(
                        "declaringFromTypes(...) requires at least one qualified name; "
                                + "omit it entirely to evaluate the predicate against every event.");
            }
            return new ToStep(new FromMatcher.PredicateBased(sourcePredicate, declared));
        }

        /**
         * Declare the {@code to} identity, leaving the predicate match unrestricted.
         *
         * @param target the {@code to} identity
         * @return a builder awaiting {@code transform(...)}
         */
        public TransformStep to(MessageType target) {
            requireNonNull(target, "target may not be null");
            return new ToStep(new FromMatcher.PredicateBased(sourcePredicate, Set.of())).to(target);
        }
    }

    /** Continuation of {@link #from(MessageType)} / {@link #from(Predicate)}; supplies {@code to(...)}. */
    final class ToStep {

        private final FromMatcher matcher;

        private ToStep(FromMatcher matcher) {
            this.matcher = matcher;
        }

        /**
         * Declare the {@code to} identity.
         *
         * @param target the {@code to} identity
         * @return a builder awaiting {@code transform(...)}
         */
        public TransformStep to(MessageType target) {
            requireNonNull(target, "target may not be null");
            return new TransformStep(matcher, target);
        }
    }

    /** Continuation of {@code from(...).to(...)}; supplies the payload mapper. */
    final class TransformStep {

        private final FromMatcher matcher;
        private final MessageType toType;

        private TransformStep(FromMatcher matcher, MessageType toType) {
            this.matcher = matcher;
            this.toType = toType;
        }

        /**
         * Supply the payload mapping behavior for a non-generic input type.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type the stored payload is converted to before invocation
         * @param payloadMapper maps the input payload and processing context to its transformed output
         * @return the resulting {@link EventTransformation}
         */
        public <T, U> EventTransformation transform(Class<T> inputType,
                                                 BiFunction<T, @Nullable ProcessingContext, U> payloadMapper) {
            requireNonNull(inputType, "inputType may not be null");
            requireNonNull(payloadMapper, "payloadMapper may not be null");
            return new MappingEventTransformation<>(matcher, toType, TypeReference.fromClass(inputType), payloadMapper);
        }

        /**
         * Supply a context-free payload mapper for a non-generic input type. Convenience overload of
         * {@link #transform(Class, BiFunction)} for transformations that derive their output purely from the
         * input payload and have no use for the {@link ProcessingContext}.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type the stored payload is converted to before invocation
         * @param payloadMapper maps the input payload to its transformed output
         * @return the resulting {@link EventTransformation}
         */
        public <T, U> EventTransformation transform(Class<T> inputType,
                                                 Function<T, U> payloadMapper) {
            requireNonNull(inputType, "inputType may not be null");
            requireNonNull(payloadMapper, "payloadMapper may not be null");
            return transform(inputType, (payload, context) -> payloadMapper.apply(payload));
        }

        /**
         * Generic-type overload. Use this when {@code inputType} carries type parameters (e.g.
         * {@code Map<String, Object>}, {@code List<Foo>}).
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the {@link TypeReference} the stored payload is converted to
         * @param payloadMapper maps the input payload and processing context to its transformed output
         * @return the resulting {@link EventTransformation}
         */
        public <T, U> EventTransformation transform(TypeReference<T> inputType,
                                                 BiFunction<T, @Nullable ProcessingContext, U> payloadMapper) {
            requireNonNull(inputType, "inputType may not be null");
            requireNonNull(payloadMapper, "payloadMapper may not be null");
            return new MappingEventTransformation<>(matcher, toType, inputType, payloadMapper);
        }

        /**
         * Context-free variant of {@link #transform(TypeReference, BiFunction)}. Use this when {@code inputType}
         * carries type parameters (e.g. {@code Map<String, Object>}, {@code List<Foo>}) and the mapper has no use
         * for the {@link ProcessingContext}.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the {@link TypeReference} the stored payload is converted to
         * @param payloadMapper maps the input payload to its transformed output
         * @return the resulting {@link EventTransformation}
         */
        public <T, U> EventTransformation transform(TypeReference<T> inputType,
                                                 Function<T, U> payloadMapper) {
            requireNonNull(inputType, "inputType may not be null");
            requireNonNull(payloadMapper, "payloadMapper may not be null");
            return transform(inputType, (payload, context) -> payloadMapper.apply(payload));
        }
    }
}
