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

package connector.kafka;

import connector.kafka.ParcelEvents.OrderShipped;
import connector.kafka.ParcelEvents.ParcelDispatched;
// tag::publisher[]
import io.axoniq.framework.kafka.publishing.EventSinks;
import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

import java.util.concurrent.CompletableFuture;

public class ParcelPublisher {

    private final EventSink kafka;

    // The Kafka sink named "outbound": config.getComponent(EventSink.class, "outbound")
    public ParcelPublisher(EventSink kafka) {
        this.kafka = kafka;
    }

    @EventHandler
    public CompletableFuture<Void> on(OrderShipped event, MessageTypeResolver typeResolver, ProcessingContext context) {
        if (!event.international()) {
            // Not every event on the inside is relevant on the outside.
            return FutureUtils.emptyCompletedFuture();
        }
        // The type is resolved by the application's MessageTypeResolver, honoring @Event: parcels.ParcelDispatched 1.0
        return EventSinks.publishPayloads(kafka, context, typeResolver,
                                          new ParcelDispatched(event.parcelId(), event.destinationCountry()));
    }
}
// end::publisher[]
