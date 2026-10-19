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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import static java.util.Objects.requireNonNull;

/**
 * A pure rename {@link EventTransformation}: it re-labels an event matched by exact identity {@code source} with
 * the new identity {@code target}, passing the stored payload, metadata, timestamp and identifier through
 * unchanged. Unlike {@link MappingEventTransformation}, a rename may change the
 * {@link org.axonframework.messaging.core.QualifiedName}, not only the version. The framework owns the new
 * identity, so neither the {@link org.axonframework.messaging.core.conversion.MessageConverter} nor the
 * {@link org.axonframework.messaging.core.MessageTypeResolver} is consulted and no output-identity check applies.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class RenameEventTransformation implements EventTransformation {

    private final MessageType source;
    private final MessageType target;
    private final FromMatcher matcher;

    /**
     * Constructs a rename from {@code source} to {@code target}.
     *
     * @param source the {@code from} identity matched by exact equality
     * @param target the {@code to} identity applied to the output
     */
    RenameEventTransformation(MessageType source, MessageType target) {
        this.source = requireNonNull(source, "source may not be null");
        this.target = requireNonNull(target, "target may not be null");
        this.matcher = new FromMatcher.Exact(source);
    }

    /**
     * Re-labels the matched message with {@link #target}, leaving the payload and the rest of the envelope
     * untouched.
     *
     * @param message the matched input message
     * @param context the per-message {@link TransformationContext}; required by the contract but otherwise unused,
     *                as a rename consults neither the converter nor the resolver
     * @return a single-element stream carrying the re-labeled message
     */
    MessageStream<EventMessage> transform(EventMessage message, TransformationContext context) {
        requireNonNull(context, "context may not be null");
        EventMessage output = new GenericEventMessage(
                message.identifier(),
                target,
                message.payload(),
                message.metadata(),
                message.timestamp()
        );
        return MessageStream.just(output);
    }

    FromMatcher matcher() {
        return matcher;
    }

    /**
     * The declared {@code to} identity applied to this rename's output.
     *
     * @return the declared {@code to} {@link MessageType}
     */
    MessageType toType() {
        return target;
    }

    @Override
    public String toString() {
        return "RenameEventTransformation{from=" + source + ", to=" + target + '}';
    }
}
