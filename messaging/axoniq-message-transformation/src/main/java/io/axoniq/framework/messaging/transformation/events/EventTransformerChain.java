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
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * An immutable, thread-safe chain of {@link EventTransformation} instances that transforms events at read time.
 * Events that match no transformation pass through unchanged. Use {@link Builder} to construct and register
 * transformations.
 * <p>
 * The chain is a thin read-time engine composing two read-models derived from the registered transformations: a
 * {@link TransformationIndex} answering which transformation applies to an event, and a {@link CriteriaWidener}
 * broadening read criteria so a type-filtering read also fetches the source types the chain transforms.
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

    /** Last-match-wins lookup of the transformation applying to an event. */
    private final TransformationIndex index;

    /** Broadens read criteria so type-filtering reads also fetch the source types this chain transforms. */
    private final CriteriaWidener widener;

    /**
     * Safety bound on per-event iteration. Guards against pathological configurations
     * (e.g., a predicate-based cycle that no static check can detect). Hitting this raises
     * {@link ChainConfigurationException}.
     */
    private final int maxIterationsPerEvent;

    private EventTransformerChain(TransformationIndex index,
                                  CriteriaWidener widener,
                                  int maxIterationsPerEvent) {
        this.index = index;
        this.widener = widener;
        this.maxIterationsPerEvent = maxIterationsPerEvent;
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
     * Widens the given read {@link EventCriteria} so a type-filtering read still returns every event this chain
     * can transform into one of the queried types. Returns the same instance when nothing can be widened.
     *
     * @param criteria the read-time criteria to widen
     * @return the widened criteria, or the same instance when nothing is broadened
     */
    public EventCriteria widen(EventCriteria criteria) {
        requireNonNull(criteria, "criteria may not be null");
        return widener.widen(criteria);
    }

    /**
     * Applies the chain to a single event, repeatedly applying the latest matching transformation until none matches.
     */
    private EventMessage applyChainToOneEvent(EventMessage event, TransformationContext context) {
        EventMessage current = event;
        for (int iteration = 0; iteration < maxIterationsPerEvent; iteration++) {
            EventTransformation match = index.findLastMatch(current.type());
            if (match == null) {
                return current;
            }
            // Only a payload mapping is constrained to a version change; other variants own their own rules.
            if (match instanceof MappingEventTransformation<?, ?> mapping) {
                rejectNameChange(current.type(), mapping.toType());
            }
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
    private static EventMessage singleResult(MessageStream<? extends EventMessage> result,
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
     * not its {@code QualifiedName}.
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

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("transformationCount", index.count());
        descriptor.describeProperty("exactTransformations", index.exactTransformationsDescription());
        descriptor.describeProperty("predicateTransformations", index.predicateTransformationsDescription());
        descriptor.describeProperty("maxIterationsPerEvent", maxIterationsPerEvent);
        descriptor.describeProperty("wideningGraph", widener.graphDescription());
        descriptor.describeProperty("typeFilterDroppingTargets", widener.typeFilterDroppingDescription());
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

        private final List<EventTransformation> transformations = new ArrayList<>();
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
            // Only a payload mapping with an exact from is held to a version-only change; other variants are exempt.
            if (transformation instanceof MappingEventTransformation<?, ?> mapping
                    && mapping.matcher() instanceof FromMatcher.Exact(MessageType source)) {
                rejectNameChange(source, mapping.toType());
            }
            transformations.add(transformation);
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
            TransformationIndex index = TransformationIndex.of(transformations);
            CriteriaWidener widener = CriteriaWidener.of(index.transformations());
            logChainContents(index);
            return new EventTransformerChain(index, widener, maxIterationsPerEvent);
        }

        private static void logChainContents(TransformationIndex index) {
            if (!logger.isDebugEnabled()) {
                return;
            }
            int count = index.count();
            if (count == 0) {
                logger.debug("EventTransformerChain built with 0 transformations (no-op pass-through).");
                return;
            }
            String summary = index.transformations()
                                  .map(EventTransformation::toString)
                                  .collect(Collectors.joining(", "));
            logger.debug("EventTransformerChain built with {} transformation(s): [{}]", count, summary);
        }
    }
}
