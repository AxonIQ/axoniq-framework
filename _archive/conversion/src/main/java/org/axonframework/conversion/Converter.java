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

package org.axonframework.conversion;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.infra.DescribableComponent;

import java.lang.reflect.Type;

/**
 * Interface describing a mechanism that can convert data from one to another type.
 * <p>
 * Used when object are added/retrieved from a storage solution or put on/received from a network. Clear example of this
 * is the {@code org.axonframework.messaging.core.Message}.
 *
 * @author Allard Buijze
 * @author Rene de Waele
 * @since 3.0.0
 */
public interface Converter extends DescribableComponent {

    /**
     * Converts the given {@code input} object into an object of the given {@code targetType}.
     *
     * @param input      The value to convert.
     * @param targetType The type to convert the given {@code input} into.
     * @param <T>        The target data type.
     * @return A converted version of the given {@code input} into the given {@code targetType}.
     * @throws ConversionException If the {@code input} cannot be converted to the given {@code targetType}.
     */
    @Nullable
    default <T> T convert(@Nullable Object input, Class<T> targetType) {
        return convert(input, (Type) targetType);
    }

    /**
     * Converts the given {@code input} object into an object of the given {@code targetType}.
     *
     * @param input      The value to convert.
     * @param targetType The type to convert the given {@code input} into.
     * @param <T>        The target data type.
     * @return A converted version of the given {@code input} into the given {@code targetType}.
     * @throws ConversionException If the {@code input} cannot be converted to the given {@code targetType}.
     */
    @Nullable
    <T> T convert(@Nullable Object input, Type targetType);
}
