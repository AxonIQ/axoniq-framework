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

import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes payloads to any {@link EventSink}, such as a {@link KafkaEventSink}, typing each payload with a
 * {@link MessageTypeResolver}.
 * <p>
 * An {@link EventSink} publishes {@link EventMessage EventMessages}, and the type of each message decides how consumers
 * recognize the event. Resolving that type with the application's {@link MessageTypeResolver} honors the
 * {@link org.axonframework.messaging.eventhandling.annotation.Event @Event} annotation on payload classes. A handler
 * receives the resolver as a parameter, and publishes to a sink its component holds, such as a Kafka sink obtained by
 * name:
 * <pre>{@code
 * @EventHandler
 * CompletableFuture<Void> on(OrderShipped event, MessageTypeResolver typeResolver, ProcessingContext context) {
 *     return EventSinks.publishPayloads(kafka, context, typeResolver,
 *                                       new ParcelDispatched(event.parcelId(), event.destinationCountry()));
 * }
 * }</pre>
 * Events that already are {@link EventMessage EventMessages} are published as they are, for example to publish with
 * metadata.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class EventSinks {

    private EventSinks() {
        // Utility class
    }

    /**
     * Publishes the given {@code events}, which are payloads or {@link EventMessage EventMessages}, to the given
     * {@code sink}. The type of each payload is resolved with the given {@code typeResolver}.
     *
     * @param sink         the sink to publish to
     * @param context      the context in which the events are published, or {@code null} when published outside of
     *                     one
     * @param typeResolver the resolver of the type of each payload, typically the application's own
     * @param events       the payloads or event messages to publish
     * @return the future that the {@code sink} returns for the published events, or a failed future when a payload's
     * type cannot be resolved
     */
    public static CompletableFuture<Void> publishPayloads(EventSink sink,
                                                          @Nullable ProcessingContext context,
                                                          MessageTypeResolver typeResolver,
                                                          Object... events) {
        return publishPayloads(sink, context, typeResolver, Arrays.asList(events));
    }

    /**
     * Publishes the given {@code events}, which are payloads or {@link EventMessage EventMessages}, to the given
     * {@code sink}. See {@link #publishPayloads(EventSink, ProcessingContext, MessageTypeResolver, Object...)}.
     *
     * @param sink         the sink to publish to
     * @param context      the context in which the events are published, or {@code null} when published outside of
     *                     one
     * @param typeResolver the resolver of the type of each payload, typically the application's own
     * @param events       the payloads or event messages to publish
     * @return the future that the {@code sink} returns for the published events, or a failed future when a payload's
     * type cannot be resolved
     */
    public static CompletableFuture<Void> publishPayloads(EventSink sink,
                                                          @Nullable ProcessingContext context,
                                                          MessageTypeResolver typeResolver,
                                                          List<?> events) {
        Objects.requireNonNull(sink, "The sink must not be null.");
        Objects.requireNonNull(typeResolver, "The typeResolver must not be null.");
        Objects.requireNonNull(events, "The events must not be null.");
        List<EventMessage> messages = new ArrayList<>(events.size());
        try {
            for (Object event : events) {
                Objects.requireNonNull(event, "An event must not be null.");
                messages.add(event instanceof EventMessage message
                                     ? message
                                     : new GenericEventMessage(typeResolver.resolveOrThrow(event), event));
            }
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return sink.publish(context, messages);
    }
}
