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

package io.axoniq.framework.kafka.codec;

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;

/**
 * The result of {@link KafkaEventCodec#decode decoding} a Kafka record: the event it carries, and the resources the
 * record carries alongside the event.
 * <p>
 * The {@code resources} hold what the codec read from the record besides the event itself, such as the event's
 * {@link org.axonframework.messaging.eventstreaming.Tag tags}. The consuming component adds them to the context of the
 * event, together with the {@link io.axoniq.framework.kafka.common.KafkaResources} that describe where the record came
 * from.
 *
 * @param event     the event the record carries
 * @param resources the resources the record carries alongside the event
 * @author Allard Buijze
 * @since 5.5.0
 */
public record DecodedRecord(EventMessage event, Context resources) {

    /**
     * Compact constructor validating that the given {@code event} and {@code resources} are not {@code null}.
     *
     * @param event     the event the record carries
     * @param resources the resources the record carries alongside the event
     */
    public DecodedRecord {
        Objects.requireNonNull(event, "The event must not be null.");
        Objects.requireNonNull(resources, "The resources must not be null.");
    }
}
