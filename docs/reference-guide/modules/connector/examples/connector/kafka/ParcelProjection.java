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

import connector.kafka.ParcelEvents.ParcelDispatched;
// tag::projection[]
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.streaming.KafkaPartitionSequencing;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ParcelProjection {

    private final Map<String, String> destinations = new ConcurrentHashMap<>();

    @KafkaPartitionSequencing // <1>
    @EventHandler
    public void on(ParcelDispatched event, ProcessingContext context) {
        TopicPartition partition = context.getResource(KafkaResources.TOPIC_PARTITION); // <2>
        destinations.put(event.parcelId(), event.destinationCountry() + " (" + partition + ")");
    }
}
// end::projection[]
