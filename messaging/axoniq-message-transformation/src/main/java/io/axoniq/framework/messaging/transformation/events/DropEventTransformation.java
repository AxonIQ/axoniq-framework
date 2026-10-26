/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.FromMatcher;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;

import static java.util.Objects.requireNonNull;

/**
 * A 1:0 drop {@link EventTransformation}: it suppresses an event matched by exact identity {@code source}, declaring
 * no {@code to} and producing no output. The chain removes a matched event by short-circuiting on this variant; a
 * direct {@link #transform(EventMessage, TransformationContext)} call yields an empty stream. A drop converts no
 * payload and resolves no identity, so neither the
 * {@link org.axonframework.messaging.core.conversion.MessageConverter} nor the
 * {@link org.axonframework.messaging.core.MessageTypeResolver} is consulted.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class DropEventTransformation implements EventTransformation {

    private final MessageType source;
    private final FromMatcher matcher;

    /**
     * Constructs a drop of events matching {@code source}.
     *
     * @param source the {@code from} identity matched by exact equality
     */
    DropEventTransformation(MessageType source) {
        this.source = requireNonNull(source, "source may not be null");
        this.matcher = new FromMatcher.Exact(source);
    }

    FromMatcher matcher() {
        return matcher;
    }

    @Override
    public String toString() {
        return "DropEventTransformation{from=" + source + ", to=(dropped)}";
    }
}
