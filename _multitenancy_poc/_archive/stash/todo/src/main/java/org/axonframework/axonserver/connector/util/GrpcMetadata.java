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

import io.axoniq.axonserver.grpc.MetaDataValue;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.conversion.Serializer;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Implementation that provides access to a {@link Map} of gRPC {@link MetaDataValue}s in the form of {@link Metadata}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Deprecated(forRemoval = true, since = "5.0.0")
public class GrpcMetadata implements Supplier<Metadata> {

    private final Map<String, MetaDataValue> metaDataValues;
    private final GrpcMetadataConverter grpcMetaDataConverter;
    private volatile Metadata metaData;

    /**
     * Instantiate a wrapper around the given {@code metaDataValues} providing access to them as a {@link Metadata}
     * object.
     *
     * @param metaDataValues a {@link Map} of {@link String} to {@link MetaDataValue} to wrap as a {@link Metadata}
     * @param serializer     the {@link Serializer} used to deserialize {@link MetaDataValue}s with
     */
    public GrpcMetadata(Map<String, MetaDataValue> metaDataValues, Serializer serializer) {
        this.metaDataValues = metaDataValues;
        this.grpcMetaDataConverter = new GrpcMetadataConverter(serializer);
    }

    @Override
    public Metadata get() {
        if (metaData == null) {
            this.metaData = grpcMetaDataConverter.convert(metaDataValues);
        }
        return metaData;
    }
}
