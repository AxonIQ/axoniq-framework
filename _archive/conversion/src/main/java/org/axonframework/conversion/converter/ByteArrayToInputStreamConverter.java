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

package org.axonframework.conversion.converter;

import org.jspecify.annotations.Nullable;
import org.axonframework.conversion.ContentTypeConverter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * A {@link ContentTypeConverter} implementation that converts {@code byte[]} into an {@link InputStream}.
 * <p>
 * More specifically, it returns an {@link ByteArrayInputStream} with the underlying {@code byte[]} is backing data.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public class ByteArrayToInputStreamConverter implements ContentTypeConverter<byte[], InputStream> {

    @Override
    public Class<byte[]> expectedSourceType() {
        return byte[].class;
    }

    @Override
    public Class<InputStream> targetType() {
        return InputStream.class;
    }

    @Override
    public @Nullable InputStream convert(byte @Nullable[] input) {
        return input != null ? new ByteArrayInputStream(input) : null;
    }
}
