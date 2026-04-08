/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.core;

import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageTypeResolver} using the {@link Class} of the given {@code payload} to base the
 * {@link MessageType type} on.
 * <p>
 * The {@link Class#getPackageName()} becomes the {@link QualifiedName#namespace()} and the
 * {@link Class#getSimpleName()} becomes the {@link QualifiedName#localName()} of the
 * {@link MessageType#qualifiedName()}. The {@link MessageType#version()} is defaulted to
 * {@link MessageType#DEFAULT_VERSION} when not specified differently through this class' constructor.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class ClassBasedMessageTypeResolver implements MessageTypeResolver {

    private final String version;

    /**
     * Constructs a {@link ClassBasedMessageTypeResolver} using {@link MessageType#DEFAULT_VERSION} as the revision for
     * all resolved {@link MessageType types}.
     */
    public ClassBasedMessageTypeResolver() {
        this(MessageType.DEFAULT_VERSION);
    }

    /**
     * Constructs a {@link ClassBasedMessageTypeResolver} using the given {@code revision} as the revision for all
     * resolved {@link MessageType types}. If payload is already a message the {@code type} of the message is used
     * without any changes.
     *
     * @param version The version for all resolved {@link MessageType types} by this {@link MessageTypeResolver}
     *                implementation.
     */
    public ClassBasedMessageTypeResolver(String version) {
        this.version = version;
    }

    @Override
    public Optional<MessageType> resolve(Class<?> payloadType) {
        Objects.requireNonNull(payloadType, "payloadType may not be null");
        return Optional.of(new MessageType(payloadType.getName(), version));
    }
}
