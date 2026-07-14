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

package io.axoniq.framework.tracing.micrometer;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.tracing.Span;

/**
 * Internal SPI implemented by every {@link Span} in this binding that carries a raw Micrometer
 * {@link io.micrometer.tracing.Span} -- {@link MicrometerSpan} itself. {@link MicrometerSpanFactory#rawSpanFrom}
 * unwraps through this SPI rather than casting to {@link MicrometerSpan} directly, so any future {@code Span}
 * implementation in this binding that carries a raw Micrometer span remains resolvable as a parent without a hard
 * cast to a specific class.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
interface RawSpanCarrier {

    /**
     * Returns the underlying, already-started raw Micrometer span.
     *
     * @return the raw Micrometer span; never {@code null}
     */
    io.micrometer.tracing.Span rawSpan();
}
