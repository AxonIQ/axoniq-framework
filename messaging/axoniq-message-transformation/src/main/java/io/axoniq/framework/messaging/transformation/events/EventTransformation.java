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

import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Factory producing {@link EventTransformer} instances. Use one of the static methods
 * ({@link #from(MessageType)} / {@link #from(Predicate)},
 * {@link #rename(MessageType, MessageType)}) and register the result with
 * {@code EventTransformerChain.builder().register(...)}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class EventTransformation {

    private EventTransformation() {
    }

    /**
     * Begin a 1:1 transformation matching the given concrete {@code from} identity by
     * exact equality. Continue with {@code to(...)} then {@code transform(...)}.
     *
     * @param source the {@code from} identity
     * @return a builder awaiting {@code to(...)}
     */
    public static SingleEventTransformationBuilder from(MessageType source) {
        throw new UnsupportedOperationException("Tests-first stub; impl lands in T023");
    }

    /**
     * Begin a 1:1 transformation matching any {@link MessageType} for which the supplied
     * predicate returns {@code true}.
     *
     * @param sourcePredicate the matcher
     * @return a builder awaiting {@code to(...)}
     */
    public static SingleEventTransformationBuilder from(Predicate<MessageType> sourcePredicate) {
        throw new UnsupportedOperationException("Tests-first stub; impl lands in T023");
    }

    /**
     * Pure identity rename: payload passes through unchanged, only the {@link MessageType}
     * is updated.
     *
     * @param source the {@code from} identity
     * @param target the {@code to} identity
     * @return the resulting {@link EventTransformer}
     */
    public static EventTransformer rename(MessageType source, MessageType target) {
        throw new UnsupportedOperationException("Tests-first stub; impl lands in T038");
    }

    /** Continuation of {@link #from(MessageType)} / {@link #from(Predicate)}; supplies {@code to(...)}. */
    public static final class SingleEventTransformationBuilder {

        SingleEventTransformationBuilder() {
        }

        /**
         * Declare the {@code to} identity.
         *
         * @param target the {@code to} identity
         * @return a builder awaiting {@code transform(...)}
         */
        public SingleEventTransformationWithTargetBuilder to(MessageType target) {
            throw new UnsupportedOperationException("Tests-first stub; impl lands in T024");
        }
    }

    /** Continuation of {@code from(...).to(...)}; supplies the payload mapper. */
    public static final class SingleEventTransformationWithTargetBuilder {

        SingleEventTransformationWithTargetBuilder() {
        }

        /**
         * Supply the payload mapping behaviour for a non-generic input type.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type the stored payload is converted to before invocation
         * @param payloadMapper maps the input payload + processing context to its transformed output
         * @return the resulting {@link EventTransformer}
         */
        public <T, U> EventTransformer transform(Class<T> inputType,
                                                  BiFunction<T, ProcessingContext, U> payloadMapper) {
            throw new UnsupportedOperationException("Tests-first stub; impl lands in T024");
        }

        /**
         * Generic-type overload. Use this when {@code inputType} carries type parameters
         * (e.g. {@code Map<String, Object>}, {@code List<Foo>}).
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the {@link TypeReference} the stored payload is converted to
         * @param payloadMapper maps the input payload + processing context to its transformed output
         * @return the resulting {@link EventTransformer}
         */
        public <T, U> EventTransformer transform(TypeReference<T> inputType,
                                                  BiFunction<T, ProcessingContext, U> payloadMapper) {
            throw new UnsupportedOperationException("Tests-first stub; impl lands in T024");
        }
    }
}
