/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.conversion.StoredMessageConverter;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.jspecify.annotations.Nullable;

/**
 * Converts the parts of a deadline that the persistent deadline managers store, its payload, metadata and scope
 * descriptor, to and from their stored form, keeping the layout Axon Framework 4.13 wrote.
 * <p>
 * The payload and metadata are converted as the {@link StoredMessageConverter} describes their stored form. A payload
 * type name that resolves to no class yields an {@link UnknownDeadlinePayload} instead of a failure, as Axon Framework
 * 4's {@code UnknownSerializedType} did. The scope descriptor is stored with its class name, which resolves as a
 * payload type name does, and failures are reported as a {@link DeadlineException}.
 * <p>
 * This class is internal, as it only serves the deadline managers of this module, which share the stored layout.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@Internal
public final class StoredDeadlineConverter extends StoredMessageConverter {

    private static final MessageTypeResolver MESSAGE_TYPE_RESOLVER = new ClassBasedMessageTypeResolver();

    /**
     * Creates a {@code StoredDeadlineConverter} converting with the given {@code converter}.
     *
     * @param converter the converter matching the stored form of the deadlines
     */
    public StoredDeadlineConverter(Converter converter) {
        super(converter);
    }

    /**
     * Returns the {@link MessageType} of a deadline carrying the given {@code payload}, derived from the payload's
     * class, as the stored form keeps no message type.
     *
     * @param payload the payload of the deadline
     * @return the message type derived from the payload's class
     */
    public static MessageType messageTypeOf(@Nullable Object payload) {
        return MESSAGE_TYPE_RESOLVER.resolveOrThrow(payload == null ? Void.class : payload.getClass());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Yields an {@link UnknownDeadlinePayload}, so that the deadline is still delivered and its handler can decide what
     * to do with it.
     */
    @Override
    protected Object unknownPayload(String typeName, @Nullable String revision, @Nullable Object data) {
        return new UnknownDeadlinePayload(typeName, revision, data);
    }

    @Override
    protected RuntimeException exception(String message, @Nullable Throwable cause) {
        return cause == null ? new DeadlineException(message) : new DeadlineException(message, cause);
    }

    /**
     * Converts the given stored scope descriptor into the class named by the given {@code className}.
     *
     * @param className the stored class name of the scope descriptor
     * @param data      the stored scope descriptor
     * @return the converted scope descriptor
     * @throws DeadlineException if the class name resolves to no {@link ScopeDescriptor} class, or the data converts to
     *                           no scope descriptor
     */
    public ScopeDescriptor scope(String className, Object data) {
        Class<?> scopeType = resolve(className).orElseThrow(() -> new DeadlineException(
                "The scope descriptor of the deadline is of the unknown class [" + className + "]"
        ));
        if (!ScopeDescriptor.class.isAssignableFrom(scopeType)) {
            throw new DeadlineException("The class [" + className + "] of the deadline's scope is no ScopeDescriptor");
        }
        return fromStored(data, scopeType.asSubclass(ScopeDescriptor.class));
    }
}
