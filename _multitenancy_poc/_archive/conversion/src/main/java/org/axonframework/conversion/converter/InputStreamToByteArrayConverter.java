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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link ContentTypeConverter} implementation that converts an {@link InputStream} to a {@code byte[]}.
 * <p>
 * This converter simply reads all contents from the input stream and returns that as an array.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public class InputStreamToByteArrayConverter implements ContentTypeConverter<InputStream, byte[]> {

    @Override
    public Class<InputStream> expectedSourceType() {
        return InputStream.class;
    }

    @Override
    public Class<byte[]> targetType() {
        return byte[].class;
    }

    @Override
    public byte @Nullable[] convert(@Nullable InputStream input) {
        if (input == null) {
            return null;
        }

        try {
            return bytesFrom(input);
        } catch (IOException e) {
            throw new ConversionException(
                    "Unable to convert InputStream to byte[]. Error while reading from Stream.", e
            );
        }
    }

    private byte[] bytesFrom(InputStream original) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(1024); // NOSONAR - There is no point in closing BAOS
        byte[] buffer = new byte[1024];
        int n;
        while (-1 != (n = original.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }
}
