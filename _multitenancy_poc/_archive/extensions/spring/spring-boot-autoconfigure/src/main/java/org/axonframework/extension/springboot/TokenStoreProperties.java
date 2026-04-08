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

package org.axonframework.extension.springboot;

import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Properties describing the settings for the default
 * {@link TokenStore Token Store}.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
@ConfigurationProperties("axon.eventhandling.tokenstore")
public class TokenStoreProperties {

    /**
     * The claim timeout is the amount of time a
     * {@link StreamingEventProcessor StreamingEventProcessor's} process will wait
     * before it forces a claim of a {@link TrackingToken}. Thus, if a claim has not
     * been updated for the given {@code claimTimeout}, this process will 'steal' the claim. Defaults to a
     * {@link Duration} of 10 seconds.
     */
    private Duration claimTimeout = Duration.ofSeconds(10);

    /**
     * Gets the claim timeout as {@link Duration}.
     * <p>
     * The claim timeout is the amount of time a
     * {@link StreamingEventProcessor StreamingEventProcessor's} process will wait
     * before it forces a claim of a {@link TrackingToken}. Thus, if a claim has not
     * been updated for the given {@code claimTimeout}, this process will 'steal' the claim. Defaults to a
     * {@link Duration} of 10 seconds.
     * <p>
     *
     * @return the claim timeout as {@link Duration}
     */
    public Duration getClaimTimeout() {
        return claimTimeout;
    }


    /**
     * Sets the claim timeout as {@link Duration}.
     * <p>
     * The claim timeout is the amount of time a
     * {@link StreamingEventProcessor StreamingEventProcessor's)} process will wait
     * before it forces a claim of a {@link TrackingToken}. Thus, if a claim has not
     * been updated for the given {@code claimTimeout}, this process will 'steal' the claim.
     * <p>
     *
     * @param claimTimeout the {@link Duration} of the default claim timeout
     */
    public void setClaimTimeout(Duration claimTimeout) {
        this.claimTimeout = claimTimeout;
    }
}
