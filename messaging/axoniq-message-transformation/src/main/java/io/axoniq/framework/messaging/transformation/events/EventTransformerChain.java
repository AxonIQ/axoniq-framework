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

import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Immutable chain of {@link EventTransformer} instances applied at event read time.
 * Built once at startup via {@link Builder} and locked on {@link Builder#build()};
 * register the chain with the Axon configuration as an
 * {@code EventTransformerChain.class}-typed component and the framework installs the
 * read-side decorators automatically. Each input event passes through the chain via
 * fixed-point iteration; when multiple transformers match, the latest registration wins.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public final class EventTransformerChain {

    /** {@link QualifiedName}-keyed lookup for transformers whose {@code from} is a concrete {@code MessageType}. */
    private final Map<QualifiedName, List<EventTransformer>> concreteFromIndex;

    /** Flat list for transformers whose {@code from} is a {@code Predicate<MessageType>}. */
    private final List<EventTransformer> predicateFromList;

    /**
     * Package-private; get instances via {@link #builder()}.
     *
     * @param concreteFromIndex {@link QualifiedName}-keyed index for concrete-{@code from} transformers
     * @param predicateFromList flat list for predicate-{@code from} transformers
     */
    EventTransformerChain(Map<QualifiedName, List<EventTransformer>> concreteFromIndex,
                          List<EventTransformer> predicateFromList) {
        this.concreteFromIndex = requireNonNull(concreteFromIndex, "concreteFromIndex");
        this.predicateFromList = requireNonNull(predicateFromList, "predicateFromList");
    }

    /**
     * Apply the chain to the given stream. Non-matching elements pass through unchanged.
     *
     * @param stream the input stream of events
     * @return the transformed stream
     */
    public MessageStream<? extends EventMessage> transform(MessageStream<EventMessage> stream) {
        // Foundational shell: returns input unchanged. Body lands with T026 / T027.
        return stream;
    }

    /**
     * Start building a new chain.
     *
     * @return a fresh {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder for {@link EventTransformerChain}. Registration order = application
     * order. Calling {@link #build()} returns an immutable, locked chain.
     */
    public static final class Builder {

        Builder() {
        }

        /**
         * Register an {@link EventTransformer} with the chain.
         *
         * @param transformer the transformer to add
         * @return this builder
         */
        public Builder register(EventTransformer transformer) {
            requireNonNull(transformer, "transformer");
            // Foundational shell: routing into the concrete-index / predicate-list lands
            // with T025. Locking + conflict detection land with T029.
            return this;
        }

        /**
         * Lock the chain and return an immutable instance.
         *
         * @return the locked chain
         */
        public EventTransformerChain build() {
            // Foundational shell: returns an empty chain. Lock flag, immutable views,
            // chain-build DEBUG entry, and duplicate-/self-loop checks land with T029.
            return new EventTransformerChain(Map.of(), List.of());
        }
    }
}
