/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.ClockUtils;
import org.axonframework.common.ObjectUtils;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Generic implementation of the {@link DeadlineMessage} interface.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @since 3.3.0
 */
public class GenericDeadlineMessage extends GenericEventMessage implements DeadlineMessage {

    private static final MessageTypeResolver MESSAGE_TYPE_RESOLVER = new ClassBasedMessageTypeResolver();

    private final String deadlineName;

    /**
     * Returns the given {@code deadlineName} and {@code messageOrPayload} as a DeadlineMessage. If the
     * {@code messageOrPayload} parameter is of type {@link Message}, a new DeadlineMessage will be created using the
     * payload and metadata of the given message. Otherwise, the given {@code messageOrPayload} is wrapped into a
     * GenericDeadlineMessage as its payload.
     *
     * @param deadlineName     the name for the resulting {@link DeadlineMessage}
     * @param messageOrPayload a {@link Message} or payload to wrap as a DeadlineMessage
     * @return a DeadlineMessage using the {@code deadlineName} as its deadline name and containing the given
     * {@code messageOrPayload} as the payload
     * @deprecated use {@link #asDeadlineMessage(String, Object, Instant)} instead, as it sets the timestamp of the
     * deadline, rather than the current time
     */
    @Deprecated
    public static DeadlineMessage asDeadlineMessage(String deadlineName, @Nullable Object messageOrPayload) {
        return asDeadlineMessage(deadlineName, messageOrPayload, ClockUtils.instant());
    }

    /**
     * Returns the given {@code deadlineName} and {@code messageOrPayload} as a DeadlineMessage which expires at the
     * given {@code expiryTime}. If the {@code messageOrPayload} parameter is of type {@link Message}, a new
     * {@code DeadlineMessage} instance will be created using the payload and metadata of the given message. Otherwise,
     * the given {@code messageOrPayload} is wrapped into a {@code GenericDeadlineMessage} as its payload, with a
     * {@link MessageType} resolved from the payload's class.
     *
     * @param deadlineName     the name for the resulting {@link DeadlineMessage}
     * @param messageOrPayload a {@link Message} or payload to wrap as a DeadlineMessage
     * @param expiryTime       the timestamp at which the deadline expires
     * @return a DeadlineMessage using the {@code deadlineName} as its deadline name and containing the given
     * {@code messageOrPayload} as the payload
     */
    public static DeadlineMessage asDeadlineMessage(String deadlineName,
                                                    @Nullable Object messageOrPayload,
                                                    Instant expiryTime) {
        if (messageOrPayload instanceof Message message) {
            return new GenericDeadlineMessage(deadlineName, message, () -> expiryTime);
        }
        MessageType type = MESSAGE_TYPE_RESOLVER.resolveOrThrow(ObjectUtils.nullSafeTypeOf(messageOrPayload));
        return new GenericDeadlineMessage(
                deadlineName, new GenericMessage(type, messageOrPayload), () -> expiryTime
        );
    }

    /**
     * Constructs a {@code GenericDeadlineMessage} for the given {@code type} and {@code deadlineName}.
     * <p>
     * The {@link #payload()} defaults to {@code null} and the {@link Metadata} defaults to an empty instance.
     *
     * @param type         the {@link MessageType type} for this {@link DeadlineMessage}
     * @param deadlineName the name for this {@link DeadlineMessage}
     */
    public GenericDeadlineMessage(MessageType type,
                                  String deadlineName) {
        this(deadlineName, type, null);
    }

    /**
     * Constructs a {@code GenericDeadlineMessage} for the given {@code deadlineName}, {@code type}, and
     * {@code payload}.
     * <p>
     * The {@link Metadata} defaults to an empty instance.
     *
     * @param deadlineName the name for this {@link DeadlineMessage}
     * @param type         the {@link MessageType type} for this {@link DeadlineMessage}
     * @param payload      the payload for this {@link DeadlineMessage}
     */
    public GenericDeadlineMessage(String deadlineName,
                                  MessageType type,
                                  @Nullable Object payload) {
        this(deadlineName, type, payload, Metadata.emptyInstance());
    }

    /**
     * Constructs a {@code GenericDeadlineMessage} for the given {@code deadlineName}, {@code type}, {@code payload},
     * and {@code metadata}.
     *
     * @param deadlineName the name for this {@link DeadlineMessage}
     * @param type         the {@link MessageType type} for this {@link DeadlineMessage}
     * @param payload      the payload for this {@link DeadlineMessage}
     * @param metadata     the metadata for this {@link DeadlineMessage}
     */
    public GenericDeadlineMessage(String deadlineName,
                                  MessageType type,
                                  @Nullable Object payload,
                                  Map<String, String> metadata) {
        super(type, payload, metadata);
        this.deadlineName = deadlineName;
    }

    /**
     * Constructs a {@code GenericDeadlineMessage} for the given {@code deadlineName}, {@code identifier}, {@code type},
     * {@code payload}, {@code metadata}, and {@code timestamp}.
     *
     * @param deadlineName the name for this {@link DeadlineMessage}
     * @param identifier   the identifier of this {@link DeadlineMessage}
     * @param type         the {@link MessageType type} for this {@link DeadlineMessage}
     * @param payload      the payload for this {@link DeadlineMessage}
     * @param metadata     the metadata for this {@link DeadlineMessage}
     * @param timestamp    the {@link Instant timestamp} of this {@link DeadlineMessage DeadlineMessage's} creation
     */
    public GenericDeadlineMessage(String deadlineName,
                                  String identifier,
                                  MessageType type,
                                  @Nullable Object payload,
                                  Map<String, String> metadata,
                                  Instant timestamp) {
        super(identifier, type, payload, metadata, timestamp);
        this.deadlineName = deadlineName;
    }

    /**
     * Constructs a {@code GenericDeadlineMessage} for the given {@code deadlineName}, {@code delegate} and
     * {@code timestampSupplier}, intended to reconstruct another {@link DeadlineMessage}.
     * <p>
     * The timestamp of the deadline is supplied lazily through the given {@code timestampSupplier} to prevent
     * unnecessary deserialization of the timestamp.
     *
     * @param deadlineName      the name for this {@link DeadlineMessage}
     * @param delegate          the {@link Message} containing {@link Message#payload() payload},
     *                          {@link Message#type() type}, {@link Message#identifier() identifier} and
     *                          {@link Message#metadata() metadata} for the {@link DeadlineMessage} to reconstruct
     * @param timestampSupplier {@link Supplier} for the {@link Instant timestamp} of the
     *                          {@link DeadlineMessage DeadlineMessage's} creation
     */
    public GenericDeadlineMessage(String deadlineName,
                                  Message delegate,
                                  Supplier<Instant> timestampSupplier) {
        super(delegate, timestampSupplier);
        this.deadlineName = deadlineName;
    }

    @Override
    public String getDeadlineName() {
        return deadlineName;
    }

    @Override
    public DeadlineMessage withMetadata(Map<String, String> metadata) {
        return new GenericDeadlineMessage(deadlineName, delegate().withMetadata(metadata), this::timestamp);
    }

    @Override
    public DeadlineMessage andMetadata(Map<String, String> additionalMetadata) {
        return new GenericDeadlineMessage(
                deadlineName, delegate().andMetadata(additionalMetadata), this::timestamp
        );
    }

    @Override
    public DeadlineMessage withConvertedPayload(Type type, Converter converter) {
        Object convertedPayload = payloadAs(type, converter);
        if (ObjectUtils.nullSafeTypeOf(convertedPayload).isAssignableFrom(payloadType())) {
            return this;
        }
        Message delegate = delegate();
        Message converted = new GenericMessage(delegate.identifier(),
                                               delegate.type(),
                                               convertedPayload,
                                               delegate.metadata());
        return new GenericDeadlineMessage(getDeadlineName(), converted, this::timestamp);
    }

    /**
     * Returns a new {@code GenericDeadlineMessage} with the same properties as this message, including its deadline
     * name, and the given {@code converter} set for use in {@link #payloadAs(Class)}.
     *
     * @param converter the converter for the new message
     * @return a copy of this instance with the converter set
     */
    @Override
    public GenericDeadlineMessage withConverter(@Nullable Converter converter) {
        Message updated = delegate() instanceof GenericMessage genericMessage
                ? genericMessage.withConverter(converter)
                : delegate();
        return new GenericDeadlineMessage(deadlineName, updated, this::timestamp);
    }

    @Override
    protected String describeType() {
        return "GenericDeadlineMessage";
    }
}
