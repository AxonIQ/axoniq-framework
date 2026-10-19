/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A {@link MessageTypeResolver} that resolves structural carrier types ({@code byte[]}, {@code Map},
 * {@code Collection}, {@code String}, {@code Number}, and JSON tree nodes) to {@link Optional#empty()},
 * delegating every other payload type to the wrapped resolver.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
public final class StructuralAwareMessageTypeResolver implements MessageTypeResolver {

    private final MessageTypeResolver delegate;

    /**
     * Wraps the given {@code delegate} resolver.
     *
     * @param delegate the resolver to delegate non-structural payload types to
     */
    public StructuralAwareMessageTypeResolver(MessageTypeResolver delegate) {
        this.delegate = requireNonNull(delegate, "delegate may not be null");
    }

    @Override
    public Optional<MessageType> resolve(Class<?> payloadType) {
        return StructuralPayloadTypes.isStructural(payloadType)
                ? Optional.empty()
                : delegate.resolve(payloadType);
    }
}
