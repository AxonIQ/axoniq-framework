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

/**
 * Interface describing a mechanism that converts an object from a specified {@link #expectedSourceType() source type} to
 * the defined {@link #targetType() target type}.
 *
 * @param <S> The expected source type for this {@code ContentTypeConverter} to {@link #convert(Object) convert}.
 * @param <T> The output type of this {@code ContentTypeConverter's} {@link #convert(Object) convert} method.
 * @author Allard Buijze
 * @since 2.0.0
 */
public interface ContentTypeConverter<S, T> {

    /**
     * Returns the expected type of input data for this {@code ContentTypeConverter} to {@link #convert(Object)}.
     *
     * @return The expected type of input data for this {@code ContentTypeConverter} to {@link #convert(Object)}.
     */
    Class<S> expectedSourceType();

    /**
     * Returns the type of output for this {@code ContentTypeConverter} to {@link #convert(Object)} into.
     *
     * @return The type of output for this {@code ContentTypeConverter} to {@link #convert(Object)} into.
     */
    Class<T> targetType();

    /**
     * Converts the given {@code input} object of generic type {@code S} into an object of generic type {@code T}.
     *
     * @param input The object of generic type {@code S} to convert into an object of generic type {@code T}.
     * @return The converted version of the given {@code input} in type {@code T}.
     */
    @Nullable T convert(@Nullable S input);
}
