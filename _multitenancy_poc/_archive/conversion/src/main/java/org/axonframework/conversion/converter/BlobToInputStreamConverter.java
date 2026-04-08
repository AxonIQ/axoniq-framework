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
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.ContentTypeConverter;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * A {@link ContentTypeConverter} implementation that converts {@link Blob} into an {@link InputStream}.
 *
 * @author Allard Buijze
 * @since 2.3.0
 */
public class BlobToInputStreamConverter implements ContentTypeConverter<Blob, InputStream> {

    @Override
    public Class<Blob> expectedSourceType() {
        return Blob.class;
    }

    @Override
    public Class<InputStream> targetType() {
        return InputStream.class;
    }

    @Override
    public @Nullable InputStream convert(@Nullable Blob input) {
        if (input == null) {
            return null;
        }

        try {
            return input.getBinaryStream();
        } catch (SQLException e) {
            throw new ConversionException("Error while attempting to read data from Blob.", e);
        }
    }
}
