/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventhandling.processing.streaming.segmenting;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Simple {@link SegmentChangeListener} implementation backed by claim and release handlers.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 */
public class SimpleSegmentChangeListener implements SegmentChangeListener {

    private final Function<Segment, CompletableFuture<Void>> onClaim;
    private final Function<Segment, CompletableFuture<Void>> onRelease;

    /**
     * Creates a listener with explicit claim and release handlers.
     *
     * @param onClaim   The claim handler.
     * @param onRelease The release handler.
     */
    public SimpleSegmentChangeListener(
            Function<Segment, CompletableFuture<Void>> onClaim,
            Function<Segment, CompletableFuture<Void>> onRelease
    ) {
        Objects.requireNonNull(onClaim, "Claim listener may not be null");
        Objects.requireNonNull(onRelease, "Release listener may not be null");
        this.onClaim = onClaim;
        this.onRelease = onRelease;
    }

    @Override
    public CompletableFuture<Void> onSegmentClaimed(Segment segment) {
        return onClaim.apply(segment);
    }

    @Override
    public CompletableFuture<Void> onSegmentReleased(Segment segment) {
        return onRelease.apply(segment);
    }
}
