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

package io.axoniq.framework.kafka.publishing;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Resolves the key of the record that a {@link KafkaEventSink} publishes an event as.
 * <p>
 * The key decides the partition a record is written to. Kafka only guarantees the order of records within a
 * partition, so events that consumers must see in order need the same key, such as the identifier of the order or
 * parcel they concern. Without a key, Kafka spreads records over the partitions, and consumers may see them in any
 * order.
 * <pre>{@code
 * RecordKeyResolver byParcel = (event, context) -> event.payloadAs(ParcelDispatched.class).parcelId();
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@FunctionalInterface
public interface RecordKeyResolver {

    /**
     * Returns a resolver that publishes every record without a key, leaving the choice of partition to Kafka.
     *
     * @return a resolver that publishes every record without a key
     */
    static RecordKeyResolver none() {
        return (event, context) -> null;
    }

    /**
     * Returns a resolver that uses the value of the given {@code metadataKey} in the event's metadata as the record
     * key. Events without that metadata entry are published without a key.
     *
     * @param metadataKey the key of the metadata entry holding the record key
     * @return a resolver that uses the given metadata entry as the record key
     */
    static RecordKeyResolver fromMetadata(String metadataKey) {
        Objects.requireNonNull(metadataKey, "The metadataKey must not be null.");
        return (event, context) -> event.metadata().get(metadataKey);
    }

    /**
     * Resolves the key of the record to publish the given {@code event} as.
     *
     * @param event   the event to publish
     * @param context the context in which the event is published, or {@code null} when published outside of one
     * @return the record key, or {@code null} to publish the record without a key
     */
    @Nullable
    String resolve(EventMessage event, @Nullable ProcessingContext context);
}
