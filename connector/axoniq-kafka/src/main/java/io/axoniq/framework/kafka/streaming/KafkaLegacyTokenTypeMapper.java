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

package io.axoniq.framework.kafka.streaming;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.LegacyTokenTypeMapper;

import java.util.Map;

/**
 * A {@link LegacyTokenTypeMapper} that lets token stores read the tokens that the Kafka extension of Axon Framework 4
 * stored, as a {@link LegacyKafkaTrackingToken}.
 * <p>
 * Registered through {@code META-INF/services}, so that token stores pick it up automatically. Internal, because it
 * is a migration bridge, not an application extension point.
 *
 * @author Allard Buijze
 * @since 5.5.0
 * @deprecated rename the stored token type to {@link LegacyKafkaTrackingToken} instead
 */
@Internal
@Deprecated(since = "5.5.0", forRemoval = true)
@SuppressWarnings("removal") // the bridge is temporary; LegacyKafkaTrackingToken works without it
public final class KafkaLegacyTokenTypeMapper implements LegacyTokenTypeMapper {

    /**
     * The class name under which Axon Framework 4 stored its Kafka tracking tokens.
     */
    public static final String LEGACY_TOKEN_TYPE =
            "org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken";

    @Override
    public Map<String, Class<? extends TrackingToken>> mappings() {
        return Map.of(LEGACY_TOKEN_TYPE, LegacyKafkaTrackingToken.class);
    }
}
