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
     * Apply the chain to the given stream using the supplied {@link MessageConverter} for
     * any input type / payload type mismatch, threading the active {@link ProcessingContext}
     * (when present) through to each matched transformer's mapper, and verifying that every
     * mapper output's resolved identity matches the transformer's declared {@code to} via
     * the supplied {@link MessageTypeResolver}. Called by the {@code TransformingEventStore}
     * decorator at production read time.
     * <p>
     * The supplied resolver is wrapped with a structural-aware decorator. For carrier types
     * such as {@code byte[]}, {@code Map}, {@code List}, {@code String}, {@code Number}, and
     * Jackson tree nodes ({@link StructuralPayloadTypes}), the wrapper returns
     * {@link java.util.Optional#empty()} so the output-identity check is skipped. This keeps
     * the guard correct for untyped mapper outputs without changing framework-wide resolver
     * behavior.
     *
     * @param stream             the input stream of events
     * @param context            the active processing context, or {@code null} on the
     *                           tracking processor read path when the caller did not supply
     *                           one
     * @param converter          the framework's payload converter
     * @param messageTypeResolver the resolver used to verify the mapper's output identity
     *                           against the declared {@code to}; if the resolver returns
     *                           {@link java.util.Optional#empty()} for the output's class
     *                           (typical for untyped representations such as {@code JsonNode}
     *                           or {@code Map}), the check is skipped
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
     * Applies the chain to a single event with fixed-point iteration: find the
     * latest-registered transformer that matches, apply it, repeat until no transformer
     * matches. The {@link TransformationContext} bundle is threaded through unchanged so each
     * matched transformer sees the same per-entry context, converter, and resolver.
     */
    private EventMessage applyChainToOneEvent(EventMessage event, TransformationContext context) {
        EventMessage current = event;
        for (int iteration = 0; iteration < maxIterationsPerEvent; iteration++) {
            var match = findLastMatch(current);
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
     * Pulls the single transformed event out of a transformer's result stream. Every 1:1
     * transformation emits exactly one synchronously-available element, so the stream is drained
     * in place; an empty stream signals a transformer that produced no output and is rejected.
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
        descriptor.describeProperty("transformationCount", transformerCount());
        descriptor.describeProperty("concreteFromIndex", concreteFromDescription());
        descriptor.describeProperty("predicateFromList", predicateFromDescription());
        descriptor.describeProperty("maxIterationsPerEvent", maxIterationsPerEvent);
    }

    private int transformerCount() {
        return predicateFromList.size()
                + concreteFromIndex.values().stream().mapToInt(List::size).sum();
    }

    /** Concrete-from buckets rendered as {@code qualifiedName -> [transformer-toString, ...]}. */
    private Map<String, List<String>> concreteFromDescription() {
        Map<String, List<String>> rendered = HashMap.newHashMap(concreteFromIndex.size());
        concreteFromIndex.forEach((qualifiedName, bucket) ->
                rendered.put(qualifiedName.name(), bucket.stream()
                                                          .map(entry -> entry.transformer().toString())
                                                          .toList()));
        return rendered;
    }

    /** Predicate-from transformers rendered by their {@code toString()}. */
    private List<String> predicateFromDescription() {
        return predicateFromList.stream().map(entry -> entry.transformer().toString()).toList();
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
         * Register an {@link EventTransformer} with the chain. Every {@link EventTransformer}
         * is produced by the {@code EventTransformation} factory ({@link EventTransformer} is
         * sealed), so the chain always receives a routable transformer carrying its
         * {@code from} and {@code to} metadata.
         *
         * @param transformer the transformer to add
         * @return this builder
         * @throws ChainConfigurationException if {@link #build()} has already been called
         */
        public Builder register(EventTransformer transformer) {
            requireNonNull(transformer, "transformer");
            if (locked) {
                throw new ChainConfigurationException(
                        "Chain is locked after build(); further registration is rejected.");
            }
            var built = (DefaultEventTransformer<?, ?>) transformer;
            var entry = new RegisteredTransformer(nextSequence++, built);
            switch (built.matcher()) {
                case FromMatcher.Concrete(var source) -> {
                    rejectNameChange(source, built.toType());
                    concreteFromIndex.computeIfAbsent(source.qualifiedName(),
                                                      ignored -> new ArrayList<>())
                                     .add(entry);
                }
                case FromMatcher.PredicateBased(var ignored) -> predicateFromList.add(entry);
            }
            return this;
        }

        /**
         * Lock the chain and return an immutable instance. Emits an INFO entry listing each
         * registered transformer's {@code from} (and {@code to} for 1:1 transformers); this is
         * the only framework-emitted log in 5.2.0.
         *
         * @return the locked chain
         */
        public EventTransformerChain build() {
            locked = true;
            EventTransformerChain chain = new EventTransformerChain(
                    concreteFromIndex, predicateFromList, maxIterationsPerEvent);
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
                            chain.concreteFromIndex.values().stream().flatMap(List::stream),
                            chain.predicateFromList.stream())
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
