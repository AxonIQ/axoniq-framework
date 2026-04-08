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

package org.axonframework.conversion.upcasting;

import java.util.stream.Stream;

/**
 * Abstract implementation of an {@link Upcaster} that eases the common process of upcasting one intermediate
 * representation to several other representations by applying a simple flat mapping function to the input stream of
 * intermediate representations.
 *
 * @author Steven van Beelen
 * @since 3.0.6
 */
public abstract class SingleEntryMultiUpcaster<T> implements Upcaster<T> {

    @Override
    public Stream<T> upcast(Stream<T> intermediateRepresentations) {
        return intermediateRepresentations.flatMap(entry -> {
            if (!canUpcast(entry)) {
                return Stream.of(entry);
            }
            return doUpcast(entry);
        });
    }

    /**
     * Checks if this upcaster can upcast the given {@code intermediateRepresentation}. If the upcaster cannot upcast
     * the representation the {@link #doUpcast(Object)} is not invoked.
     *
     * @param intermediateRepresentation the intermediate object representation to upcast
     * @return {@code true} if the representation can be upcast, {@code false} otherwise
     */
    protected abstract boolean canUpcast(T intermediateRepresentation);

    /**
     * Upcasts the given {@code intermediateRepresentation}. This method is only invoked if {@link #canUpcast(Object)}
     * returned {@code true} for the given representation.
     * <p>
     * Note that the returned representation should not be {@code null}. To remove an intermediateRepresentation add a
     * filter to the input stream.
     *
     * @param intermediateRepresentation the representation of the object to upcast
     * @return the upcasted representations as a {@code Stream} with generic type {@code T}
     */
    protected abstract Stream<T> doUpcast(T intermediateRepresentation);
}
