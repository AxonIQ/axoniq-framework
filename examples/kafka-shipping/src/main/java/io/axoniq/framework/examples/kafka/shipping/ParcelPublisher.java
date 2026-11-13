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

package io.axoniq.framework.examples.kafka.shipping;

import io.axoniq.framework.examples.kafka.ParcelDispatched;
import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes a {@link ParcelDispatched} event to Kafka for every international shipment.
 * <p>
 * The internal {@link OrderShipped} event never leaves the service. This handler decides which shipments are relevant
 * to other systems, and what they get to see. It publishes through the Kafka sink named {@code parcels}, which Spring
 * injects when qualified with that name; an unqualified {@link EventSink} would be the application's own event bus.
 */
@Component
public class ParcelPublisher {

    private final EventSink kafka;

    public ParcelPublisher(@Qualifier("parcels") EventSink kafka) {
        this.kafka = kafka;
    }

    @EventHandler
    public CompletableFuture<Void> on(OrderShipped event,
                                      MessageTypeResolver messageTypes,
                                      ProcessingContext context) {
        if (!event.international()) {
            return FutureUtils.emptyCompletedFuture();
        }
        ParcelDispatched payload = new ParcelDispatched(event.parcelId(), event.destinationCountry());
        // The resolver honors the @Event annotation, so the published type is parcels.ParcelDispatched, version 1.0.
        GenericEventMessage published = new GenericEventMessage(
                messageTypes.resolveOrThrow(payload),
                payload,
                Map.of("parcelId", event.parcelId())
        );
        return kafka.publish(context, published);
    }
}
