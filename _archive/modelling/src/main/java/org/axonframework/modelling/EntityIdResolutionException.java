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

package org.axonframework.modelling;

import java.util.List;

/**
 * Thrown when an {@link EntityIdResolver} was unable to determine an id from
 * a given payload.
 *
 * @author John Hendrikx
 * @since 5.0.0
 */
public class EntityIdResolutionException extends Exception {

    /**
     * Constructs a new instance.
     *
     * @param cls The payload type, cannot be {@code null}
     * @param identifiers The extracted identifiers, cannot be {@code null}, or contain exactly 1 element
     * @throws IllegalArgumentException When any argument is {@code null}, or when {@code identifiers}
     *         contains exactly one element.
     */
    public EntityIdResolutionException(Class<?> cls, List<Object> identifiers) {
        super(createMessage(cls, identifiers));
    }

    private static String createMessage(Class<?> cls, List<Object> identifiers) {
        if (identifiers == null || identifiers.size() == 1) {
            throw new IllegalArgumentException("identifiers cannot be null or contain exactly one element: " + identifiers);
        }

        return "Unable to resolve id for payload of type [%s]: %s".formatted(
            cls,
            identifiers.isEmpty() ? "found no identifiers" : "found multiple identifiers: %s".formatted(identifiers)
        );
    }
}
