/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.function.BiFunction;

/**
 * The mapping of one {@link EdgeSnapshotTransformation edge}, lifting entity state of its {@link #inputType()} into
 * a new shape.
 *
 * @param inputType the type the state is converted to before mapping
 * @param mapper    the mapping function, returning {@code null} to decline
 * @param <T>       the shape of the state before mapping
 * @param <U>       the shape produced
 * @author John Hendrikx
 * @since 5.5.0
 */
record SnapshotMapping<T, U>(Class<T> inputType, BiFunction<T, @Nullable ProcessingContext, @Nullable U> mapper) {

    /*
     * Maps the given state, converting it to the input type first when it is not already of that type. Returns null
     * when the mapper declines.
     */
    @Nullable Object apply(Object state, Converter converter, @Nullable ProcessingContext context) {
        T input = inputType.isInstance(state) ? inputType.cast(state) : converter.convert(state, inputType);

        return mapper.apply(input, context);
    }
}
