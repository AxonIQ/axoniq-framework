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
import io.axoniq.framework.messaging.transformation.FromMatcher;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.BuilderUtils;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * An immutable, thread-safe chain of {@link EventTransformation} instances that transforms events at read time.
 * Events that match no transformation pass through unchanged. Use {@link Builder} to construct and register
 * transformations.
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
     * Exact-{@code from} transformations, bucketed by {@link QualifiedName}. Each bucket is
     * stored in registration order; the per-transformation {@code sequence} preserves the
     * registration order across this map AND {@link #predicateTransformations}.
     */
    private final Map<QualifiedName, List<RegisteredTransformation>> exactTransformations;

    /** Predicate-{@code from} transformations in registration order. */
    private final List<RegisteredTransformation> predicateTransformations;

    /**
     * Safety bound on per-event iteration. Guards against pathological configurations
     * (e.g., a predicate-based cycle that no static check can detect). Hitting this raises
     * {@link ChainConfigurationException}.
     */
    private final int maxIterationsPerEvent;

    private EventTransformerChain(Map<QualifiedName, List<RegisteredTransformation>> exactTransformations,
                                  List<RegisteredTransformation> predicateTransformations,
                                  int maxIterationsPerEvent) {
        this.exactTransformations = copyImmutable(exactTransformations);
        this.predicateTransformations = List.copyOf(predicateTransformations);
        this.maxIterationsPerEvent = maxIterationsPerEvent;
    }

    private static Map<QualifiedName, List<RegisteredTransformation>> copyImmutable(
            Map<QualifiedName, List<RegisteredTransformation>> source) {
        Map<QualifiedName, List<RegisteredTransformation>> copy = HashMap.newHashMap(source.size());
        source.forEach((key, bucket) -> copy.put(key, List.copyOf(bucket)));
        return Map.copyOf(copy);
    }

    /**
     * Transforms every event in the given stream by applying the chain at read time. Events matching no
     * transformation pass through unchanged.
     *
     * @param stream              the input stream of events
     * @param context             the active processing context, or {@code null} when the read path supplies none
     * @param converter           converts a stored payload to a transformation's declared input type
     * @param messageTypeResolver resolves a mapper output's type to verify it against the declared {@code to}
     * @return the transformed stream
     */
    public MessageStream<EventMessage> transform(MessageStream<? extends EventMessage> stream,
                                                 @Nullable ProcessingContext context,
                                                 MessageConverter converter,
                                                 MessageTypeResolver messageTypeResolver) {
        requireNonNull(converter, "converter may not be null");
        requireNonNull(messageTypeResolver, "messageTypeResolver may not be null");
        // Entry-level map (not mapMessage) keeps the engine-attached Context in scope so the
        // chain can include the stream position in diagnostic exceptions.
        return stream.map(entry -> entry.map(event -> applyChainToOneEvent(
                event, new TransformationContext(entry, context, converter, messageTypeResolver))));
    }

    /**
     * Applies the chain to a single event, repeatedly applying the latest matching transformation until none matches.
     */
    private EventMessage applyChainToOneEvent(EventMessage event, TransformationContext context) {
        EventMessage current = event;
        for (int iteration = 0; iteration < maxIterationsPerEvent; iteration++) {
            MappingEventTransformation<?, ?> match = findLastRegisteredMatch(current);
            if (match == null) {
                return current;
            }
            rejectNameChange(current.type(), match.toType());
            current = singleResult(match.transform(current, context), current, context);
        }
        throw new ChainConfigurationException("""
                Chain exceeded %d iterations on a single event; \
                likely a cyclic or self-matching transformation (raise the bound via \
                Builder.maxIterationsPerEvent(int) if your domain genuinely has more hops). \
                Last event: %s""".formatted(
                maxIterationsPerEvent,
                EventDescriptions.describe(current, context.entryContext())));
    }

    /**
     * Extracts the single transformed event from a transformation's result stream, rejecting an empty result.
     */
    private static EventMessage singleResult(MessageStream<EventMessage> result,
                                             EventMessage input,
                                             TransformationContext context) {
        return result.first().next()
                     .map(MessageStream.Entry::message)
                     .orElseThrow(() -> new ChainConfigurationException("""
                             Transformation produced no output for a 1:1 transformation. \
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
     * Returns the latest-registered transformation that matches the event, or {@code null} if
     * none matches. Scans only the relevant exact bucket and the predicate list, picking
     * whichever has the higher overall registration order.
     */
    private @Nullable MappingEventTransformation<?, ?> findLastRegisteredMatch(EventMessage event) {
        MessageType eventType = event.type();
        List<RegisteredTransformation> exactBucket = exactTransformations.get(eventType.qualifiedName());
        if (exactBucket == null && predicateTransformations.isEmpty()) {
            return null;
        }
        RegisteredTransformation lastExact =
                exactBucket == null ? null : findLastRegisteredMatchIn(exactBucket, eventType);
        RegisteredTransformation lastPredicate = findLastRegisteredMatchIn(predicateTransformations, eventType);
        return pickByRegistrationOrder(lastExact, lastPredicate);
    }

    private static @Nullable RegisteredTransformation findLastRegisteredMatchIn(List<RegisteredTransformation> bucket,
                                                                   MessageType eventType) {
        for (int index = bucket.size() - 1; index >= 0; index--) {
            RegisteredTransformation candidate = bucket.get(index);
            if (candidate.transformation().matcher().matches(eventType)) {
                return candidate;
            }
        }
        return null;
    }

    private static @Nullable MappingEventTransformation<?, ?> pickByRegistrationOrder(
            @Nullable RegisteredTransformation exactCandidate,
            @Nullable RegisteredTransformation predicateCandidate) {
        if (exactCandidate == null) {
            return predicateCandidate == null ? null : predicateCandidate.transformation();
        }
        if (predicateCandidate == null) {
            return exactCandidate.transformation();
        }
        return exactCandidate.sequence() > predicateCandidate.sequence()
                ? exactCandidate.transformation()
                : predicateCandidate.transformation();
    }

    /**
     * Exposes the chain's populated structure for framework diagnostics
     * ({@code AxonConfiguration.describe(...)} / Spring Boot Actuator endpoints): the
     * registered-transformation count, the exact-{@code from} fan-out per
     * {@link QualifiedName}, the predicate-{@code from} fan-out, and the safety bound.
     */
    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("transformationCount", transformationCount());
        descriptor.describeProperty("exactTransformations", exactTransformationsDescription());
        descriptor.describeProperty("predicateTransformations", predicateTransformationsDescription());
        descriptor.describeProperty("maxIterationsPerEvent", maxIterationsPerEvent);
    }

    private int transformationCount() {
        return predicateTransformations.size()
                + exactTransformations.values().stream().mapToInt(List::size).sum();
    }

    /** Exact-from buckets rendered as {@code qualifiedName -> [transformation-toString, ...]}. */
    private Map<String, List<String>> exactTransformationsDescription() {
        Map<String, List<String>> rendered = HashMap.newHashMap(exactTransformations.size());
        exactTransformations.forEach((qualifiedName, bucket) ->
                rendered.put(qualifiedName.name(), bucket.stream()
                                                          .map(entry -> entry.transformation().toString())
                                                          .toList()));
        return rendered;
    }

    /** Predicate-from transformations rendered by their {@code toString()}. */
    private List<String> predicateTransformationsDescription() {
        return predicateTransformations.stream().map(entry -> entry.transformation().toString()).toList();
    }

    /**
     * Start building a new chain. Use {@link Builder#register(EventTransformation)} to add
     * transformations and, optionally, {@link Builder#maxIterationsPerEvent(int)} to adjust the per-event
     * iteration safety bound (default {@link #DEFAULT_MAX_ITERATIONS_PER_EVENT}).
     *
     * @return a fresh {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder for {@link EventTransformerChain}. Registration order is application order.
     * {@link #build()} produces a new, immutable {@link EventTransformerChain}; configuration is
     * startup-only, so {@link #register(EventTransformation)} and {@link #maxIterationsPerEvent(int)}
     * calls after {@code build()} are rejected.
     * <p>
     * {@link #maxIterationsPerEvent(int)} caps how many times the chain re-applies transformations to a
     * single event during fixed-point chaining (default
     * {@link EventTransformerChain#DEFAULT_MAX_ITERATIONS_PER_EVENT}); it guards against a self-matching
     * or cyclic configuration and otherwise never fires.
     * <p>
     * Not thread-safe: build the chain on a single thread at startup, then share the
     * resulting {@link EventTransformerChain} (which is immutable and concurrent-safe).
     */
    public static final class Builder {

        private final Map<QualifiedName, List<RegisteredTransformation>> exactTransformations = new HashMap<>();
        private final List<RegisteredTransformation> predicateTransformations = new ArrayList<>();
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
         * @throws ChainConfigurationException if {@link #build()} has already been called
         * @throws AxonConfigurationException  if {@code max} is not strictly positive
         */
        public Builder maxIterationsPerEvent(int max) {
            assertNotBuilt();
            BuilderUtils.assertStrictPositive(max, "maxIterationsPerEvent must be strictly positive");
            this.maxIterationsPerEvent = max;
            return this;
        }

        /**
         * Register an {@link EventTransformation} with the chain.
         *
         * @param transformation the transformation to add
         * @return this builder
         * @throws ChainConfigurationException if {@link #build()} has already been called
         */
        public Builder register(EventTransformation transformation) {
            assertNotBuilt();
            requireNonNull(transformation, "transformation may not be null");
            MappingEventTransformation<?, ?> built = (MappingEventTransformation<?, ?>) transformation;
            RegisteredTransformation entry = new RegisteredTransformation(nextSequence++, built);
            switch (built.matcher()) {
                case FromMatcher.Exact(MessageType source) -> {
                    rejectNameChange(source, built.toType());
                    exactTransformations.computeIfAbsent(source.qualifiedName(),
                                                      ignored -> new ArrayList<>())
                                     .add(entry);
                }
                case FromMatcher.PredicateBased ignored -> predicateTransformations.add(entry);
            }
            return this;
        }

        /**
         * Rejects configuration after {@link #build()}. The chain is built once at startup and handed off as an
         * immutable component, so a configuration call on an already-built builder would silently never reach the
         * wired chain; failing fast surfaces the misuse instead.
         *
         * @throws ChainConfigurationException if {@link #build()} has already been called
         */
        private void assertNotBuilt() {
            if (alreadyBuilt) {
                throw new ChainConfigurationException(
                        "Builder is already built; all configuration must happen before build().");
            }
        }

        /**
         * Builds and returns the immutable chain.
         *
         * @return the built chain
         */
        public EventTransformerChain build() {
            alreadyBuilt = true;
            EventTransformerChain chain = new EventTransformerChain(
                    exactTransformations, predicateTransformations, maxIterationsPerEvent);
            logChainContents(chain);
            return chain;
        }

        private static void logChainContents(EventTransformerChain chain) {
            if (!logger.isDebugEnabled()) {
                return;
            }
            int count = chain.transformationCount();
            if (count == 0) {
                logger.debug("EventTransformerChain built with 0 transformations (no-op pass-through).");
                return;
            }
            String summary = Stream.concat(
                            chain.exactTransformations.values().stream().flatMap(List::stream),
                            chain.predicateTransformations.stream())
                    .map(entry -> entry.transformation().toString())
                    .collect(Collectors.joining(", "));
            logger.debug("EventTransformerChain built with {} transformation(s): [{}]", count, summary);
        }
    }

    /**
     * Holds a registered transformation plus its global registration order. The {@code sequence}
     * lets {@link #findLastRegisteredMatch} pick the latest match across the exact index and the
     * predicate list without keeping a parallel flat list.
     */
    private record RegisteredTransformation(long sequence, MappingEventTransformation<?, ?> transformation) {
    }
}
