/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

/**
 * Converter that can convert an {@link EventMessage} to a {@link DeadLetterEventEntry} and vice versa.
 * <p>
 * Tracking tokens and aggregate data (only if the legacy aggregate approach is used: aggregate identifier, type, and
 * sequence number) are stored and restored as {@link Context} resources.
 *
 * @param <M> the type of the {@link EventMessage} this converter converts
 * @author Gerard Klijs
 * @since 4.7.0
 */
public interface DeadLetterMongoConverter<M extends EventMessage> {

    /**
     * Converts an {@link EventMessage} and its associated {@link Context} to a {@link DeadLetterEventEntry}.
     * <p>
     * The context is used to extract the tracking token and aggregate information, if present. When the context is
     * {@code null}, no resources are extracted and the corresponding fields of the resulting entry are {@code null}.
     *
     * @param message          the message to convert
     * @param context          the context containing resources such as the tracking token and aggregate information
     * @param eventConverter   the {@link EventConverter} converting the payload and metadata
     * @param genericConverter the {@link Converter} converting the tracking token, if present
     * @return the created {@link DeadLetterEventEntry}
     */
    DeadLetterEventEntry convert(M message,
                                 @Nullable Context context,
                                 EventConverter eventConverter,
                                 Converter genericConverter);

    /**
     * Converts a {@link DeadLetterEventEntry} to a {@link MessageStream.Entry} containing the {@link EventMessage} and
     * a {@link Context} with the restored resources.
     *
     * @param entry            the entry to convert
     * @param eventConverter   the {@link EventConverter} used to convert the payload and metadata back
     * @param genericConverter the {@link Converter} used to convert the tracking token back, if present
     * @return a {@link MessageStream.Entry} containing the message and the context with restored resources
     */
    MessageStream.Entry<M> convert(DeadLetterEventEntry entry,
                                   EventConverter eventConverter,
                                   Converter genericConverter);
}
