/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
