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

package io.axoniq.framework.messaging.eventstreaming.safepoint;

import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.HandlerAwareProcessorCustomization;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link ConfigurationEnhancer} that wires {@link SegmentSafePointProvider}s into every
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor
 * PooledStreamingEventProcessor} by registering an {@link HandlerAwareProcessorCustomization} component.
 * <p>
 * Per-processor, the customization walks each assigned {@link EventHandlingComponent}'s describe graph via
 * {@link CapturingComponentDescriptor} to discover {@link SegmentSafePointProvider}s &mdash; including
 * providers wrapped by decorator chains. When one or more providers are found, the customization registers a
 * {@link SegmentChangeListener} that, on claim, merges every provider's reported safe point into the lowest
 * non-null position via {@link TrackingToken#lowerBound(TrackingToken)}. If no providers are present, no
 * listener is added and the processor behaves identically to stock Axon Framework.
 * <p>
 * Discovery is scoped to each processor's own handler list, so providers belonging to other processors are
 * never observed here.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public class SafePointConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The order of this enhancer. Set lower than
     * {@code DeadLetterQueueConfigurationEnhancer.ENHANCER_ORDER} ({@code Integer.MAX_VALUE - 100}) so the
     * safe-point customization is registered before any DLQ customization. The discovery itself runs at
     * processor-build time, so the order does not affect correctness &mdash; only the ordering of
     * customization registrations.
     */
    public static final int ENHANCER_ORDER = Integer.MAX_VALUE - 200;

    private static final String CUSTOMIZATION_NAME = "SafePointHandlerAwareProcessorCustomization";

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerComponent(
                ComponentDefinition
                        .ofTypeAndName(HandlerAwareProcessorCustomization.class, CUSTOMIZATION_NAME)
                        .withInstance(SafePointConfigurationEnhancer::installSafePointListener)
        );
    }

    private static PooledStreamingEventProcessorConfiguration installSafePointListener(
            Configuration config,
            String processorName,
            List<EventHandlingComponent> handlers,
            PooledStreamingEventProcessorConfiguration processorConfig
    ) {
        List<SegmentSafePointProvider> providers = discoverProviders(handlers);
        if (providers.isEmpty()) {
            return processorConfig;
        }
        processorConfig.addSegmentChangeListener(
                SegmentChangeListener.onClaimWithReset(segment -> mergedSafePoint(providers, segment))
        );
        return processorConfig;
    }

    /**
     * Discovers every {@link SegmentSafePointProvider} reachable from {@code handlers} by walking each
     * handler's describe graph through {@link CapturingComponentDescriptor}.
     */
    static List<SegmentSafePointProvider> discoverProviders(List<? extends EventHandlingComponent> handlers) {
        CapturingComponentDescriptor<EventHandlingComponent> descriptor =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class);
        handlers.forEach(descriptor::capture);
        return descriptor.captured()
                         .filter(SegmentSafePointProvider.class::isInstance)
                         .map(SegmentSafePointProvider.class::cast)
                         .toList();
    }

    /**
     * Computes the merged safe point across the given {@code providers} for the supplied {@code segment} by
     * combining their reported safe points via {@link #lowerBoundOfReported(TrackingToken, TrackingToken)}.
     */
    static CompletableFuture<@Nullable TrackingToken> mergedSafePoint(
            List<SegmentSafePointProvider> providers,
            Segment segment
    ) {
        CompletableFuture<@Nullable TrackingToken> merged = CompletableFuture.completedFuture(null);
        for (SegmentSafePointProvider provider : providers) {
            CompletableFuture<@Nullable TrackingToken> reported = provider.onSegmentClaimed(segment);
            merged = merged.thenCombine(reported, SafePointConfigurationEnhancer::lowerBoundOfReported);
        }
        return merged;
    }

    /**
     * Returns the {@link TrackingToken#lowerBound(TrackingToken) lower bound} of two <em>reported</em> safe
     * points, treating {@code null} as <strong>"no safe point reported"</strong> &mdash; not as the start of
     * the stream.
     * <p>
     * This deliberately departs from the conventional {@link TrackingToken} null-semantics. Here, {@code null}
     * comes from a {@link SegmentSafePointProvider} that declined to constrain the safe point for this
     * segment, and from the accumulator's "nothing reported yet" seed. Folding such absences as if they were
     * "start of stream" would force every processor to rewind to position zero whenever any provider had
     * nothing to say &mdash; the opposite of the intended behaviour.
     */
    private static @Nullable TrackingToken lowerBoundOfReported(@Nullable TrackingToken first,
                                                                @Nullable TrackingToken second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return first.lowerBound(second);
    }
}
