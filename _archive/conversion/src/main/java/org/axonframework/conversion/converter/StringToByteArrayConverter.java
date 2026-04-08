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

import java.nio.charset.StandardCharsets;

/**
 * A {@link ContentTypeConverter} implementation that converts {@code Strings} into {@code byte[]}.
 * <p>
 * Conversion is done using the {@link StandardCharsets#UTF_8 UTF-8 character set}.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public class StringToByteArrayConverter implements ContentTypeConverter<String, byte[]> {

    @Override
    public Class<String> expectedSourceType() {
        return String.class;
    }

    @Override
    public Class<byte[]> targetType() {
        return byte[].class;
    }

    @Override
    public byte @Nullable[] convert(@Nullable String input) {
        return input != null ? input.getBytes(StandardCharsets.UTF_8) : null;
    }
}
