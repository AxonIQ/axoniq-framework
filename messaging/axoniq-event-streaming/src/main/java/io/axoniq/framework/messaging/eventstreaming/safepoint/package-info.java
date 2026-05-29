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

/**
 * Safe-point support for {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor
 * PooledStreamingEventProcessor}s.
 * <p>
 * Event handling components that maintain durable state outside the processor's tracking token (e.g.
 * projections that batch-flush to a downstream store) can implement
 * {@link io.axoniq.framework.messaging.eventstreaming.safepoint.SegmentSafePointProvider
 * SegmentSafePointProvider} to declare a <em>safe point</em>: the earliest position from which they need
 * events replayed to reach a consistent state on segment claim. The
 * {@link io.axoniq.framework.messaging.eventstreaming.safepoint.SafePointConfigurationEnhancer
 * SafePointConfigurationEnhancer} (loaded via {@code ServiceLoader}) wires the providers into every pooled
 * streaming event processor automatically through the
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.HandlerAwareProcessorCustomization
 * HandlerAwareProcessorCustomization} hook, per-processor and decorator-transparent.
 */
@NullMarked
package io.axoniq.framework.messaging.eventstreaming.safepoint;

import org.jspecify.annotations.NullMarked;
