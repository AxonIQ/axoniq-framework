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

import org.axonframework.conversion.Converter;
import org.axonframework.conversion.SerializedObject;
import org.axonframework.conversion.SerializedType;
import org.axonframework.conversion.Serializer;

/**
 * Wrapper that allows clients to access a gRPC {@link io.axoniq.axonserver.grpc.SerializedObject} message as a {@link
 * SerializedObject}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 * @deprecated By shifting from the {@link Serializer} to the {@link Converter}, this class becomes obsolete.
 */
@Deprecated(forRemoval = true, since = "5.0.0")
public class GrpcSerializedObject implements SerializedObject<byte[]> {

    private final io.axoniq.axonserver.grpc.SerializedObject payload;

    /**
     * Initialize a {@link GrpcSerializedObject}, wrapping a {@link io.axoniq.axonserver.grpc.SerializedObject} as a
     * {@link SerializedObject}.
     *
     * @param serializedObject a {@link io.axoniq.axonserver.grpc.SerializedObject} which will be wrapped as a {@link
     *                         SerializedObject}
     */
    public GrpcSerializedObject(io.axoniq.axonserver.grpc.SerializedObject serializedObject) {
        this.payload = serializedObject;
    }

    @Override
    public Class<byte[]> getContentType() {
        return byte[].class;
    }

    @Override
    public SerializedType getType() {
        return new SerializedType() {
            @Override
            public String getName() {
                return payload.getType();
            }

            @Override
            public String getRevision() {
                String revision = payload.getRevision();
                return "".equals(revision) ? null : revision;
            }
        };
    }

    @Override
    public byte[] getData() {
        return payload.getData().toByteArray();
    }
}
