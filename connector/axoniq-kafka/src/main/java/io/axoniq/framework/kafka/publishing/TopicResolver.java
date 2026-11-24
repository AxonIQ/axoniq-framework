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
import java.util.Optional;

/**
 * Resolves the topic that a {@link KafkaEventSink} publishes an event to.
 * <p>
 * An empty result means the event is not published at all. That makes a resolver a filter as well: it can route
 * events of different types to different topics, and leave out events that do not belong on any topic.
 * <pre>{@code
 * TopicResolver resolver = (event, context) -> switch (event.type().name()) {
 *     case "com.example.ParcelDispatched" -> Optional.of("parcels");
 *     case "com.example.InvoiceSent" -> Optional.of("invoices");
 *     default -> Optional.empty();
 * };
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@FunctionalInterface
public interface TopicResolver {

    /**
     * Returns a resolver that publishes every event to the given {@code topic}.
     *
     * @param topic the topic to publish every event to
     * @return a resolver that publishes every event to the given {@code topic}
     */
    static TopicResolver fixed(String topic) {
        Optional<String> result = Optional.of(Objects.requireNonNull(topic, "The topic must not be null."));
        return (event, context) -> result;
    }

    /**
     * Resolves the topic to publish the given {@code event} to.
     *
     * @param event   the event to publish
     * @param context the context in which the event is published, or {@code null} when published outside of one
     * @return the topic to publish the event to, or an empty {@link Optional} to not publish the event
     */
    Optional<String> resolve(EventMessage event, @Nullable ProcessingContext context);
}
