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

package org.axonframework.axonserver.connector.util;

import org.axonframework.messaging.core.Message;
import org.axonframework.conversion.SerializedObject;
import org.axonframework.conversion.Serializer;

import java.util.function.Function;

/**
 * Mapping that translates a {@link Message} into a GRPC {@link io.axoniq.axonserver.grpc.SerializedObject}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Deprecated(forRemoval = true, since = "5.0.0")
public class GrpcPayloadSerializer implements Function<Message, io.axoniq.axonserver.grpc.SerializedObject> {

    private final GrpcObjectSerializer<Message> delegate;

    /**
     * Constructs a {@link GrpcPayloadSerializer} using the given {@code serializer} to serialize messages with
     *
     * @param serializer the {@link Serializer} used to serialize messages with
     */
    public GrpcPayloadSerializer(Serializer serializer) {
        this(new GrpcObjectSerializer.Serializer<Message>() {
            @Override
            public <T> SerializedObject<T> serialize(Message object, Class<T> expectedRepresentation) {
                return serializer.serialize(object.payload(), expectedRepresentation);
            }
        });
    }

    private GrpcPayloadSerializer(GrpcObjectSerializer.Serializer<Message> messageSerializer) {
        this(new GrpcObjectSerializer<>(messageSerializer));
    }

    private GrpcPayloadSerializer(GrpcObjectSerializer<Message> delegate) {
        this.delegate = delegate;
    }

    @Override
    public io.axoniq.axonserver.grpc.SerializedObject apply(Message message) {
        return delegate.apply(message);
    }
}
