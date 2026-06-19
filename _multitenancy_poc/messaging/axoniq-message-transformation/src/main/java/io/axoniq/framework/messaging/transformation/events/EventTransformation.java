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
import org.axonframework.common.AxonConfigurationException;
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
 * An event-specific {@link MessageTransformation} describing how stored events of one {@link MessageType} are
 * rewritten into another when they are read. A transformation is either a 1:1 payload mapping, a pure rename,
 * or a 1:0 drop.
 * <p>
 * A mapping is built through one of two {@code from} paths, each continuing with {@code to(...)} to declare the
 * resulting identity and {@code transform(...)} to supply the payload mapper:
 * <ul>
 *     <li><b>Concrete</b> ({@link #from(MessageType)}, or {@link #from(Set)} to cover several known versions
 *     with one mapper): matches a source {@link MessageType} by exact equality. The source type is known up
 *     front. <b>Prefer this path whenever the source identities are known.</b> Exact matches are resolved by a
 *     constant-time identity lookup and let the chain widen read criteria to precisely those source types, so
 *     they impose no per-event scanning cost.</li>
 *     <li><b>Predicate-based</b> ({@link #from(Predicate)}): matches every {@link MessageType} for which the
 *     supplied predicate returns {@code true}. Reach for this only when the source identities cannot be
 *     enumerated up front: each predicate is evaluated against non-exact events in registration order, a
 *     per-event cost that grows with the number of predicates, and the widened read criteria are necessarily
 *     broader than the concrete path's. Both can carry a significant performance penalty.</li>
 * </ul>
 * A pure rename is built with {@link #rename(MessageType, MessageType)}: it leaves the payload unchanged and, unlike
 * the mapping paths, may change the {@link QualifiedName} rather than only the version. A drop is built with
 * {@link #drop(MessageType)}: matched events are removed from the read stream while their stream position is still
 * advanced.
 * <pre>{@code
 * // Concrete mapping: rewrite a single, known source type.
 * EventTransformation.from(new MessageType("com.example.CourseCreated", "1.0.0"))
 *                    .to(new MessageType("com.example.CourseCreated", "2.0.0"))
 *                    .transform(String.class, (payload, context) -> payload);
 *
 * // Predicate-based mapping: match many source types, declaring them so reads stay type-filtered.
 * EventTransformation.from(type -> "1.0.0".equals(type.version()))
 *                    .declaringFromTypes(new QualifiedName("com.example.CourseCreated"))
 *                    .to(new MessageType("com.example.CourseCreated", "2.0.0"))
 *                    .transform(String.class, (payload, context) -> payload);
 *
 * // Pure rename: same payload, new identity.
 * EventTransformation.rename(new MessageType("com.example.CourseCreated", "1.0.0"),
 *                            new MessageType("com.example.CourseRegistered", "1.0.0"));
 *
 * // Drop: remove matched events from the read stream.
 * EventTransformation.drop(new MessageType("com.example.CourseCreated", "1.0.0"));
 * }</pre>
 * <p>
 * <b>Why the declared {@code from} types matter.</b> When entities are sourced, read criteria are widened so a
 * query for the {@code to} type also returns events still stored under the {@code from} type(s) this transformation
 * rewrites. For the concrete path the source type is always known. For the predicate path the matched types cannot
 * be enumerated, so the source types must be declared explicitly through
 * {@link PredicateFromStep#declaringFromTypes(QualifiedName...)}. Declaring an incomplete set means a read for the
 * {@code to} type is not widened to the omitted types, leaving entities without events they expect to receive.
 * Omitting {@code declaringFromTypes} entirely is the safe fallback: the read's type filter is dropped for that
 * target, and matching falls back to tags, broader but never missing events.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public sealed interface EventTransformation extends MessageTransformation<EventMessage>
        permits MappingEventTransformation, RenameEventTransformation, DropEventTransformation {

    /**
     * Begin a 1:1 transformation matching the given {@code from} identity by exact equality. Continue
     * with {@code to(...)} then {@code transform(...)}.
     *
     * @param source the {@code from} identity
     * @return a builder awaiting {@code to(...)}
     */
    static ToStep from(MessageType source) {
        requireNonNull(source, "source may not be null");
        return new ToStep(FromMatcher.Exact.of(source));
    }

    /**
     * Begin a 1:1 transformation matching any of the given {@code sources} by exact equality, mapping each with a
     * single mapper. Use this to cover several known versions at once. Continue with {@code to(...)} then
     * {@code transform(...)}.
     *
     * @param sources the {@code from} identities to match; at least one is required. Order is irrelevant: the
     *                identities are matched by exact equality and held as a set
     * @return a builder awaiting {@code to(...)}
     * @throws AxonConfigurationException if {@code sources} is empty
     */
    static ToStep from(Set<MessageType> sources) {
        requireNonNull(sources, "sources may not be null");
        if (sources.isEmpty()) {
            throw new AxonConfigurationException("An exact matcher requires at least one source.");
        }
        return new ToStep(new FromMatcher.Exact(sources));
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
     * Create a pure rename of events from {@code source} to {@code target}, leaving the payload unchanged.
     * Unlike {@link #from(MessageType)}, a rename may change the {@link QualifiedName}, not only the version.
     *
     * @param source the {@code from} identity to match
     * @param target the {@code to} identity applied to the output
     * @return the completed rename {@link EventTransformation}, ready to register without further builder steps
     * @throws IllegalArgumentException if {@code source} and {@code target} are identical
     */
    static EventTransformation rename(MessageType source, MessageType target) {
        requireNonNull(source, "source may not be null");
        requireNonNull(target, "target may not be null");
        if (source.equals(target)) {
            throw new IllegalArgumentException(
                    "A rename must change the identity, but source and target are identical: " + source);
        }
        return new RenameEventTransformation(source, target);
    }

    /**
     * Drop events of identity {@code source} from the read stream, so no handler receives them. A dropped event's
     * stream position is still advanced, so a streaming processor resumes after it rather than reprocessing it.
     *
     * @param source the identity to drop
     * @return the drop transformation
     */
    static EventTransformation drop(MessageType source) {
        requireNonNull(source, "source may not be null");
        return new DropEventTransformation(source);
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
         * <p>
         * The declared names double as the source types that widen read criteria when sourcing entities (see the
         * class-level documentation): list every type this transformation rewrites, otherwise a read for the
         * {@code to} type is not widened to the omitted ones and entities miss those events.
         *
         * @param declaredFromTypes the {@code from} type names to match; at least one is required
         * @return a builder awaiting {@code to(...)}
         * @throws IllegalArgumentException if {@code declaredFromTypes} is empty
         */
        public ToStep declaringFromTypes(QualifiedName... declaredFromTypes) {
            requireNonNull(declaredFromTypes, "declaredFromTypes may not be null");
            Set<QualifiedName> declared = LinkedHashSet.newLinkedHashSet(declaredFromTypes.length);
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

        private static final String INPUT_TYPE_NOT_NULL = "inputType may not be null";
        private static final String PAYLOAD_MAPPER_NOT_NULL = "payloadMapper may not be null";

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
            requireNonNull(inputType, INPUT_TYPE_NOT_NULL);
            requireNonNull(payloadMapper, PAYLOAD_MAPPER_NOT_NULL);
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
            requireNonNull(inputType, INPUT_TYPE_NOT_NULL);
            requireNonNull(payloadMapper, PAYLOAD_MAPPER_NOT_NULL);
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
            requireNonNull(inputType, INPUT_TYPE_NOT_NULL);
            requireNonNull(payloadMapper, PAYLOAD_MAPPER_NOT_NULL);
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
            requireNonNull(inputType, INPUT_TYPE_NOT_NULL);
            requireNonNull(payloadMapper, PAYLOAD_MAPPER_NOT_NULL);
            return transform(inputType, (payload, context) -> payloadMapper.apply(payload));
        }
    }
}
