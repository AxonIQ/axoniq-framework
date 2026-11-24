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

package io.axoniq.framework.examples.kafka.tracking;

import io.axoniq.framework.examples.kafka.ParcelDispatched;
import io.axoniq.framework.kafka.streaming.KafkaPartitionSequencing;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks dispatched parcels, reading {@link ParcelDispatched} events from Kafka.
 * <p>
 * The processor of this package reads the {@code parcel-stream} source, see {@code application.properties}. Events of
 * the same Kafka partition are handled in order, and partitions are handled concurrently.
 */
@Component
public class ParcelTracking {

    private final Map<String, String> destinations = new ConcurrentHashMap<>();

    @KafkaPartitionSequencing
    @EventHandler
    public void on(ParcelDispatched event) {
        destinations.put(event.parcelId(), event.destinationCountry());
    }

    /**
     * Returns the destination of the parcel with the given {@code parcelId}, once it is dispatched.
     *
     * @param parcelId the parcel to look up
     * @return the destination country, or an empty {@link Optional} when the parcel is not dispatched
     */
    public Optional<String> destinationOf(String parcelId) {
        return Optional.ofNullable(destinations.get(parcelId));
    }
}
