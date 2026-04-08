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
import org.axonframework.common.infra.ComponentDescriptor;

import java.lang.reflect.Type;

/**
 * A {@link Converter} implementation that only "passes through" input object if the {@code sourceType} and
 * {@code targetType} are the identical.
 * <p>
 * As such, no conversion is performed by this {@code Converter}! Both {@link #convert(Object, Class)} and
 * {@link #convert(Object, Type)} will expect identical typing too, otherwise resulting in an
 * {@link IllegalArgumentException}.
 * <p>
 * As such, this {@code Converter} is only useful when conversion is not necessary (e.g. during testing) for the
 * component at hand.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public final class PassThroughConverter implements Converter {

    /**
     * The single instance of the {@code PassThroughConverter}.
     */
    public static final PassThroughConverter INSTANCE = new PassThroughConverter();

    private PassThroughConverter() {
        // Private constructor to enforce use of constant.
    }

    @Override
    @Nullable
    public <T> T convert(@Nullable Object input, Type targetType) {
        if (input == null) {
            return null;
        }
        Class<?> sourceType = input.getClass();
        if (sourceType.equals(targetType)) {
            //noinspection unchecked
            return (T) input;
        }
        throw new IllegalArgumentException(
                "This Converter only supports same-type conversion, while the unidentical source type ["
                        + sourceType + "] and target type [" + targetType.getTypeName()
                        + "] have been given."
        );
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        // Nothing internal to describe about this component
    }
}
