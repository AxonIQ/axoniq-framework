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
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * An immutable, thread-safe chain of {@link EventTransformer} instances that transforms events at read time.
 * Events that match no transformer pass through unchanged. Use {@link Builder} to construct and register
 * transformers.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public final class EventTransformerChain implements DescribableComponent {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Default safety bound on per-event iteration. Covers any reasonable migration chain
     * (most chains are 1-5 hops); deep-history domains can override via
     * {@link Builder#maxIterationsPerEvent(int)}.
     */
    public static final int DEFAULT_MAX_ITERATIONS_PER_EVENT = 100;

    /**
     * Concrete-{@code from} transformers, bucketed by {@link QualifiedName}. Each bucket is
     * stored in registration order; the per-transformer {@code sequence} preserves the
     * registration order across this map AND {@link #predicateTransformers}.
     */
    private final Map<QualifiedName, List<RegisteredTransformer>> concreteTransformers;

    /** Predicate-{@code from} transformers in registration order. */
    private final List<RegisteredTransformer> predicateTransformers;

    /**
     * Safety bound on per-event iteration. Guards against pathological configurations
     * (e.g., a predicate-based cycle that no static check can detect). Hitting this raises
     * {@link ChainConfigurationException}.
     */
    private final int maxIterationsPerEvent;

    private EventTransformerChain(Map<QualifiedName, List<RegisteredTransformer>> concreteTransformers,
                                  List<RegisteredTransformer> predicateTransformers,
                                  int maxIterationsPerEvent) {
        this.concreteTransformers = copyImmutable(concreteTransformers);
        this.predicateTransformers = List.copyOf(predicateTransformers);
        this.maxIterationsPerEvent = maxIterationsPerEvent;
    }

    private static Map<QualifiedName, List<RegisteredTransformer>> copyImmutable(
            Map<QualifiedName, List<RegisteredTransformer>> source) {
        Map<QualifiedName, List<RegisteredTransformer>> copy = HashMap.newHashMap(source.size());
        source.forEach((key, bucket) -> copy.put(key, List.copyOf(bucket)));
        return Map.copyOf(copy);
    }

    /**
     * Transforms every event in the given stream by applying the chain at read time. Events matching no
     * transformer pass through unchanged.
     *
     * @param stream              the input stream of events
     * @param context             the active processing context, or {@code null} when the read path supplies none
     * @param converter           converts a stored payload to a transformer's declared input type
     * @param messageTypeResolver resolves a mapper output's type to verify it against the declared {@code to}
     * @return the transformed stream
     */
    public MessageStream<EventMessage> transform(MessageStream<? extends EventMessage> stream,
                                                 @Nullable ProcessingContext context,
                                                 MessageConverter converter,
                                                 MessageTypeResolver messageTypeResolver) {
        requireNonNull(converter, "converter");
        requireNonNull(messageTypeResolver, "messageTypeResolver");
        MessageTypeResolver structuralAwareResolver = payloadClass ->
                StructuralPayloadTypes.isStructural(payloadClass)
                        ? Optional.empty()
                        : messageTypeResolver.resolve(payloadClass);
        // Entry-level map (not mapMessage) keeps the engine-attached Context in scope so the
        // chain can include the stream position in diagnostic exceptions.
        return stream.map(entry -> entry.map(event -> applyChainToOneEvent(
                event, new TransformationContext(entry, context, converter, structuralAwareResolver))));
    }

    /**
     * Applies the chain to a single event, repeatedly applying the latest matching transformer until none matches.
     */
    private EventMessage applyChainToOneEvent(EventMessage event, TransformationContext context) {
        EventMessage current = event;
        for (int iteration = 0; iteration < maxIterationsPerEvent; iteration++) {
            DefaultEventTransformer<?, ?> match = findLastMatch(current);
            if (match == null) {
                return current;
            }
            rejectNameChange(current.type(), match.toType());
            current = singleResult(match.transform(current, context), current, context);
        }
        throw new ChainConfigurationException("""
                Chain exceeded %d iterations on a single event; \
                likely a cyclic or self-matching transformer (raise the bound via \
                Builder.maxIterationsPerEvent(int) if your domain genuinely has more hops). \
                Last event: %s""".formatted(
                maxIterationsPerEvent,
                EventDescriptions.describe(current, context.entryContext())));
    }

    /**
     * Extracts the single transformed event from a transformer's result stream, rejecting an empty result.
     */
    private static EventMessage singleResult(MessageStream<EventMessage> result,
                                             EventMessage input,
                                             TransformationContext context) {
        return result.first().next()
                     .map(MessageStream.Entry::message)
                     .orElseThrow(() -> new ChainConfigurationException("""
                             Transformer produced no output for a 1:1 transformation. \
                             Input event: %s""".formatted(
                             EventDescriptions.describe(input, context.entryContext()))));
    }

    /**
     * Rejects a rename: a transformation may only change the version of a {@link MessageType},
     * not its {@link QualifiedName}.
     *
     * @param from the {@code from} identity
     * @param to   the declared {@code to} identity
     * @throws ChainConfigurationException if the {@code from} and {@code to} qualified names differ
     */
    private static void rejectNameChange(MessageType from, MessageType to) {
        if (!from.qualifiedName().equals(to.qualifiedName())) {
            throw new ChainConfigurationException("""
                    Event renaming is not supported: from=%s and to=%s have different \
                    qualified names. A transformation may only change the version of the message type, not the name.""".formatted(
                    from, to));
        }
    }

    /**
     * Returns the latest-registered transformer that matches the event, or {@code null} if
     * none matches. Scans only the relevant concrete bucket and the predicate list, picking
     * whichever has the higher overall registration order.
     */
    private @Nullable DefaultEventTransformer<?, ?> findLastMatch(EventMessage event) {
        MessageType eventType = event.type();
        List<RegisteredTransformer> concreteBucket = concreteTransformers.get(eventType.qualifiedName());
        if (concreteBucket == null && predicateTransformers.isEmpty()) {
            return null;
        }
        RegisteredTransformer lastConcrete =
                concreteBucket == null ? null : findLastMatchIn(concreteBucket, eventType);
        RegisteredTransformer lastPredicate = findLastMatchIn(predicateTransformers, eventType);
        return pickByRegistrationOrder(lastConcrete, lastPredicate);
    }

    private static @Nullable RegisteredTransformer findLastMatchIn(List<RegisteredTransformer> bucket,
                                                                   MessageType eventType) {
        for (int index = bucket.size() - 1; index >= 0; index--) {
            RegisteredTransformer candidate = bucket.get(index);
            if (candidate.transformer().matcher().matches(eventType)) {
                return candidate;
            }
        }
        return null;
    }

    private static @Nullable DefaultEventTransformer<?, ?> pickByRegistrationOrder(
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
     * Exposes the chain's populated structure for framework diagnostics
     * ({@code AxonConfiguration.describe(...)} / Spring Boot Actuator endpoints): the
     * registered-transformation count, the concrete-{@code from} fan-out per
     * {@link QualifiedName}, the predicate-{@code from} fan-out, and the safety bound.
     */
    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("transformerCount", transformerCount());
        descriptor.describeProperty("concreteTransformers", concreteTransformersDescription());
        descriptor.describeProperty("predicateTransformers", predicateTransformersDescription());
        descriptor.describeProperty("maxIterationsPerEvent", maxIterationsPerEvent);
    }

    private int transformerCount() {
        return predicateTransformers.size()
                + concreteTransformers.values().stream().mapToInt(List::size).sum();
    }

    /** Concrete-from buckets rendered as {@code qualifiedName -> [transformer-toString, ...]}. */
    private Map<String, List<String>> concreteTransformersDescription() {
        Map<String, List<String>> rendered = HashMap.newHashMap(concreteTransformers.size());
        concreteTransformers.forEach((qualifiedName, bucket) ->
                rendered.put(qualifiedName.name(), bucket.stream()
                                                          .map(entry -> entry.transformer().toString())
                                                          .toList()));
        return rendered;
    }

    /** Predicate-from transformers rendered by their {@code toString()}. */
    private List<String> predicateTransformersDescription() {
        return predicateTransformers.stream().map(entry -> entry.transformer().toString()).toList();
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
     * Fluent builder for {@link EventTransformerChain}. Registration order is application order.
     * {@link #build()} produces a new, immutable {@link EventTransformerChain}; registration is
     * startup-only, so {@link #register(EventTransformer)} calls after {@code build()} are rejected.
     * <p>
     * Not thread-safe: build the chain on a single thread at startup, then share the
     * resulting {@link EventTransformerChain} (which is immutable and concurrent-safe).
     */
    public static final class Builder {

        private final Map<QualifiedName, List<RegisteredTransformer>> concreteTransformers = new HashMap<>();
        private final List<RegisteredTransformer> predicateTransformers = new ArrayList<>();
        private long nextSequence = 0;
        private boolean alreadyBuilt = false;
        private int maxIterationsPerEvent = DEFAULT_MAX_ITERATIONS_PER_EVENT;

        private Builder() {
        }

        /**
         * Sets the per-event iteration safety bound. Defaults to {@link #DEFAULT_MAX_ITERATIONS_PER_EVENT}.
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
         * Register an {@link EventTransformer} with the chain.
         *
         * @param transformer the transformer to add
         * @return this builder
         * @throws ChainConfigurationException if {@link #build()} has already been called
         */
        public Builder register(EventTransformer transformer) {
            requireNonNull(transformer, "transformer");
            if (alreadyBuilt) {
                throw new ChainConfigurationException(
                        "register() must be called before build(); this builder has already been built.");
            }
            DefaultEventTransformer<?, ?> built = (DefaultEventTransformer<?, ?>) transformer;
            RegisteredTransformer entry = new RegisteredTransformer(nextSequence++, built);
            switch (built.matcher()) {
                case FromMatcher.Concrete(MessageType source) -> {
                    rejectNameChange(source, built.toType());
                    concreteTransformers.computeIfAbsent(source.qualifiedName(),
                                                      ignored -> new ArrayList<>())
                                     .add(entry);
                }
                case FromMatcher.PredicateBased ignored -> predicateTransformers.add(entry);
            }
            return this;
        }

        /**
         * Builds and returns the immutable chain.
         *
         * @return the built chain
         */
        public EventTransformerChain build() {
            alreadyBuilt = true;
            EventTransformerChain chain = new EventTransformerChain(
                    concreteTransformers, predicateTransformers, maxIterationsPerEvent);
            logChainContents(chain);
            return chain;
        }

        private static void logChainContents(EventTransformerChain chain) {
            if (!logger.isInfoEnabled()) {
                return;
            }
            int count = chain.transformerCount();
            if (count == 0) {
                logger.info("EventTransformerChain built with 0 transformations (no-op pass-through).");
                return;
            }
            String summary = Stream.concat(
                            chain.concreteTransformers.values().stream().flatMap(List::stream),
                            chain.predicateTransformers.stream())
                    .map(entry -> entry.transformer().toString())
                    .collect(Collectors.joining(", "));
            logger.info("EventTransformerChain built with {} transformation(s): [{}]", count, summary);
        }
    }

    /**
     * Holds a registered transformer plus its global registration order. The {@code sequence}
     * lets {@link #findLastMatch} pick the latest match across the concrete index and the
     * predicate list without keeping a parallel flat list.
     */
    private record RegisteredTransformer(long sequence, DefaultEventTransformer<?, ?> transformer) {
    }
}
