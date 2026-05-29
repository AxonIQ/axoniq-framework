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

import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Declares a <em>safe point</em>: the earliest {@link TrackingToken} from which a component needs events
 * replayed to reach a consistent state with the external resources it manages.
 * <p>
 * Components that maintain their own durable state separately from the event processor's tracking token
 * (for example, projections that batch-flush to an external store, or workflow engines that asynchronously
 * commit per-instance progress) implement this interface alongside {@link EventHandlingComponent}. When a
 * segment is claimed, the {@link SafePointConfigurationEnhancer} queries each provider for its safe point
 * and, if any safe point is strictly behind the segment's stored tracking token, wraps that token in a
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken ReplayToken} so the
 * segment streams from the safe point instead of the stored position.
 * <p>
 * <strong>Contract &mdash; derive from external state:</strong> the returned safe point MUST be derived from
 * externally committed state (e.g., the last position the external system has acknowledged having received
 * from this handler). It MUST NOT be derived from the processor's stored tracking token; doing so creates a
 * chicken-and-egg loop where the processor never advances past the safe point.
 * <p>
 * <strong>Performance &mdash; fast and non-blocking:</strong> {@link #onSegmentClaimed(Segment) onSegmentClaimed}
 * is invoked on the processor's coordination thread and blocks segment claiming. Implementations should
 * perform fast, in-memory or local lookups only. Long-running external calls will stall claim cycles for
 * all segments managed by the processor.
 * <p>
 * <strong>Decorator-transparency contract for third parties:</strong> safe-point providers are discovered by
 * walking {@link EventHandlingComponent} decorator chains via the
 * {@link org.axonframework.common.infra.ComponentDescriptor ComponentDescriptor} graph. Custom decorators that
 * are not built on
 * {@link org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent DelegatingEventHandlingComponent}
 * must call {@code descriptor.describeWrapperOf(delegate)} (or expose the delegate via
 * {@code describeProperty}) for safe-point discovery to traverse through them.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface SegmentSafePointProvider {

    /**
     * Invoked when a segment is claimed by the processor, before any events are dispatched. Returns the safe
     * point this component needs the segment to start from to be in a consistent state, or {@code null} if
     * the component has no safe-point requirement for the given segment.
     * <p>
     * The returned future runs on the coordination thread; implementations must complete it quickly.
     *
     * @param segment the segment being claimed
     * @return the safe point (or {@code null}) wrapped in a {@link CompletableFuture}
     */
    CompletableFuture<@Nullable TrackingToken> onSegmentClaimed(Segment segment);
}
