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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Immutable chain of {@link EventTransformer} instances applied at event read time. Built
 * once at startup via {@link Builder} and locked on {@link Builder#build()}; register the
 * chain with the Axon configuration as an {@code EventTransformerChain.class}-typed
 * component and the framework installs the read-side decorators automatically. Each input
 * event passes through the chain via fixed-point iteration; when multiple transformers
 * match, the latest registration wins.
 * <p>
 * Once built, the chain is immutable and safe to invoke concurrently from any number of
 * threads.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public final class EventTransformerChain {

    /**
     * Default safety bound on per-event iteration. Covers any reasonable migration chain
     * (most chains are 1-5 hops); deep-history domains can override via
     * {@link Builder#maxIterationsPerEvent(int)}.
     */
    public static final int DEFAULT_MAX_ITERATIONS_PER_EVENT = 100;

    /**
     * Concrete-{@code from} transformers, bucketed by {@link QualifiedName}. Each bucket is
     * stored in registration order; the per-transformer {@code sequence} preserves the
     * registration order across this map AND {@link #predicateFromList}.
     */
    private final Map<QualifiedName, List<RegisteredTransformer>> concreteFromIndex;

    /** Predicate-{@code from} transformers in registration order. */
    private final List<RegisteredTransformer> predicateFromList;

    /**
     * Safety bound on per-event iteration. Guards against pathological configurations
     * (e.g., a predicate-based cycle that no static check can detect). Hitting this raises
     * {@link ChainConfigurationException}.
     */
    private final int maxIterationsPerEvent;

    private EventTransformerChain(Map<QualifiedName, List<RegisteredTransformer>> concreteFromIndex,
                                  List<RegisteredTransformer> predicateFromList,
                                  int maxIterationsPerEvent) {
        this.concreteFromIndex = copyImmutable(concreteFromIndex);
        this.predicateFromList = List.copyOf(predicateFromList);
        this.maxIterationsPerEvent = maxIterationsPerEvent;
    }

    private static Map<QualifiedName, List<RegisteredTransformer>> copyImmutable(
            Map<QualifiedName, List<RegisteredTransformer>> source) {
        Map<QualifiedName, List<RegisteredTransformer>> copy = HashMap.newHashMap(source.size());
        source.forEach((key, bucket) -> copy.put(key, List.copyOf(bucket)));
        return Map.copyOf(copy);
    }

    /**
     * Apply the chain to the given stream. Non-matching elements pass through unchanged.
     *
     * @param stream the input stream of events
     * @return the transformed stream
     */
    public MessageStream<? extends EventMessage> transform(MessageStream<EventMessage> stream) {
        return stream.mapMessage(this::applyChainToOneEvent);
    }

    /**
     * Applies the chain to a single event with fixed-point iteration: find the
     * latest-registered transformer that matches, apply it, repeat until no transformer
     * matches.
     */
    private EventMessage applyChainToOneEvent(EventMessage event) {
        EventMessage current = event;
        for (int iteration = 0; iteration < maxIterationsPerEvent; iteration++) {
            var match = findLastMatch(current);
            if (match == null) {
                return current;
            }
            current = match.applyTo(current, null);
        }
        throw new ChainConfigurationException(
                "Chain exceeded " + maxIterationsPerEvent + " iterations on a single event; "
                        + "likely a cyclic or self-matching transformer (raise the bound via "
                        + "Builder.maxIterationsPerEvent(int) if your domain genuinely has more hops). "
                        + "Last event type: " + current.type());
    }

    /**
     * Returns the latest-registered transformer that matches the event, or {@code null} if
     * none matches. Scans only the relevant concrete bucket and the predicate list, picking
     * whichever has the higher overall registration order.
     */
    private @Nullable BuiltEventTransformer findLastMatch(EventMessage event) {
        var eventType = event.type();
        var concreteBucket = concreteFromIndex.get(eventType.qualifiedName());
        if (concreteBucket == null && predicateFromList.isEmpty()) {
            return null;
        }
        var lastConcrete = concreteBucket == null ? null : findLastMatchIn(concreteBucket, eventType);
        var lastPredicate = findLastMatchIn(predicateFromList, eventType);
        return pickByRegistrationOrder(lastConcrete, lastPredicate);
    }

    private static @Nullable RegisteredTransformer findLastMatchIn(List<RegisteredTransformer> bucket,
                                                                   MessageType eventType) {
        for (int index = bucket.size() - 1; index >= 0; index--) {
            var candidate = bucket.get(index);
            if (candidate.transformer().matcher().matches(eventType)) {
                return candidate;
            }
        }
        return null;
    }

    private static @Nullable BuiltEventTransformer pickByRegistrationOrder(
            @Nullable RegisteredTransformer concreteCandidate,
            @Nullable RegisteredTransformer predicateCandidate) {
        if (concreteCandidate == null) {
            return predicateCandidate == null ? null : predicateCandidate.transformer();
        }
        if (predicateCandidate == null) {
            return concreteCandidate.transformer();
        }
        return concreteCandidate.sequence() > predicateCandidate.sequence()
                ? concreteCandidate.transformer()
                : predicateCandidate.transformer();
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
     * order. Calling {@link #build()} returns an immutable, locked chain; further
     * registration is rejected.
     * <p>
     * Not thread-safe: build the chain on a single thread at startup, then share the
     * resulting {@link EventTransformerChain} (which is immutable and concurrent-safe).
     */
    public static final class Builder {

        private final Map<QualifiedName, List<RegisteredTransformer>> concreteFromIndex = new HashMap<>();
        private final List<RegisteredTransformer> predicateFromList = new ArrayList<>();
        private long nextSequence = 0;
        private boolean locked = false;
        private int maxIterationsPerEvent = DEFAULT_MAX_ITERATIONS_PER_EVENT;

        private Builder() {
        }

        /**
         * Raise the per-event safety bound above {@link #DEFAULT_MAX_ITERATIONS_PER_EVENT}.
         * Set this when a legitimate long migration chain (e.g., a deep-history domain) needs
         * more hops than the default.
         *
         * @param max the new safety bound, strictly positive
         * @return this builder
         * @throws IllegalArgumentException if {@code max < 1}
         */
        public Builder maxIterationsPerEvent(int max) {
            if (max < 1) {
                throw new IllegalArgumentException(
                        "maxIterationsPerEvent must be >= 1, got " + max);
            }
            this.maxIterationsPerEvent = max;
            return this;
        }

        /**
         * Register an {@link EventTransformer} with the chain. The transformer must have
         * been produced by the {@code EventTransformation} factory; raw lambdas are not
         * accepted because they carry no routing metadata.
         *
         * @param transformer the transformer to add
         * @return this builder
         * @throws ChainConfigurationException if {@link #build()} has already been called or
         *                                     if the transformer was not produced by the factory
         */
        public Builder register(EventTransformer transformer) {
            requireNonNull(transformer, "transformer");
            if (locked) {
                throw new ChainConfigurationException(
                        "Chain is locked after build(); further registration is rejected.");
            }
            if (!(transformer instanceof BuiltEventTransformer built)) {
                throw new ChainConfigurationException(
                        "Transformer must be produced by EventTransformation factory; "
                                + "raw EventTransformer instances cannot be registered with the chain.");
            }
            var entry = new RegisteredTransformer(nextSequence++, built);
            switch (built.matcher()) {
                case FromMatcher.Concrete(var source) ->
                        concreteFromIndex.computeIfAbsent(source.qualifiedName(),
                                                          ignored -> new ArrayList<>())
                                         .add(entry);
                case FromMatcher.PredicateBased(var ignored) -> predicateFromList.add(entry);
            }
            return this;
        }

        /**
         * Lock the chain and return an immutable instance.
         *
         * @return the locked chain
         */
        public EventTransformerChain build() {
            locked = true;
            return new EventTransformerChain(concreteFromIndex, predicateFromList, maxIterationsPerEvent);
        }
    }

    /**
     * Holds a registered transformer plus its global registration order. The {@code sequence}
     * lets {@link #findLastMatch} pick the latest match across the concrete index and the
     * predicate list without keeping a parallel flat list.
     */
    private record RegisteredTransformer(long sequence, BuiltEventTransformer transformer) {
    }
}
