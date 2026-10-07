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

package io.axoniq.framework.messaging.eventhandling.deadletter.jpa;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.ClassUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter responsible for converting to and from {@link EventMessage} implementations for storage in a
 * {@link io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue}.
 * <p>
 * Tracking tokens and aggregate data (only if legacy Aggregate approach is used: aggregate identifier, type, sequence
 * number) are stored as {@link Context} resources. This converter extracts these resources from the context during
 * serialization and restores them to the context when deserializing.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class EventMessageDeadLetterJpaConverter implements DeadLetterJpaConverter<EventMessage> {

    // Uses concrete HashMap i.o. Map to ensure default-typing Converters can deal with this ref accordingly
    private static final TypeReference<HashMap<String, String>> METADATA_MAP_TYPE_REF = new TypeReference<>() {
    };

    @Override
    public DeadLetterEventEntry convert(EventMessage message,
                                        @Nullable Context context,
                                        EventConverter eventConverter,
                                        Converter genericConverter) {
        Context effectiveContext = context != null ? context : Context.empty();
        TrackingToken token = effectiveContext.getResource(TrackingToken.RESOURCE_KEY);

        return new DeadLetterEventEntry(
                message.type().toString(),
                message.identifier(),
                message.timestamp().toString(),
                eventConverter.convert(message.payload(), byte[].class),
                eventConverter.convert(message.metadata(), byte[].class),
                effectiveContext.getResource(LegacyResources.AGGREGATE_TYPE_KEY),
                effectiveContext.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY),
                effectiveContext.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY),
                token != null ? token.getClass().getName() : null,
                token != null ? genericConverter.convert(token, byte[].class) : null
        );
    }

    @Override
    public MessageStream.Entry<EventMessage> convert(DeadLetterEventEntry entry,
                                                     EventConverter eventConverter,
                                                     Converter genericConverter) {
        return new SimpleEntry<>(deserializeMessage(entry, eventConverter),
                                 restoreContext(entry, genericConverter));
    }

    private EventMessage deserializeMessage(DeadLetterEventEntry entry, EventConverter eventConverter) {
        Map<String, String> metadataMap = eventConverter.convert(entry.getMetadata(), METADATA_MAP_TYPE_REF.getType());

        return new GenericEventMessage(
                entry.getIdentifier(),
                MessageType.fromString(entry.getType()),
                entry.getPayload(),
                Metadata.from(metadataMap),
                Instant.parse(entry.getTimestamp())
        ).withConverter(eventConverter);
    }

    private Context restoreContext(DeadLetterEventEntry entry, Converter genericConverter) {
        Context context = Context.empty();
        if (entry.getToken() != null && entry.getTokenType() != null) {
            TrackingToken token = genericConverter.convert(entry.getToken(),
                                                           ClassUtils.loadClass(entry.getTokenType()));
            if (token != null) {
                context = context.withResource(TrackingToken.RESOURCE_KEY, token);
            }
        }
        if (entry.getAggregateIdentifier() != null) {
            context = context.withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY,
                                           entry.getAggregateIdentifier());
        }
        if (entry.getAggregateType() != null) {
            context = context.withResource(LegacyResources.AGGREGATE_TYPE_KEY, entry.getAggregateType());
        }
        if (entry.getAggregateSequenceNumber() != null) {
            context = context.withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY,
                                           entry.getAggregateSequenceNumber());
        }
        return context;
    }
}
