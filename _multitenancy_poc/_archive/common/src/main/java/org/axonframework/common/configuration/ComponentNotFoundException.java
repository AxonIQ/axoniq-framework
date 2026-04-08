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

package org.axonframework.common.configuration;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.TypeReference;

/**
 * A {@code RuntimeException} dedicated when a {@link Component} cannot be found in the {@link Configuration}.
 *
 * @author Steven van Beelen
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class ComponentNotFoundException extends RuntimeException {

    /**
     * Constructs a {@code ComponentNotFoundException} with a default message describing a {@link Component} couldn't be
     * found for the given {@code type} and {@code name}.
     *
     * @param type the type of the component that could not be found, typically an interface
     * @param name the name of the component that could not be found, potentially {@code null} when unimportant
     */
    public ComponentNotFoundException(Class<?> type, @Nullable String name) {
        super(exceptionMessageFor(type, name));
    }

    /**
     * Constructs a {@code ComponentNotFoundException} with a default message describing a {@link Component} couldn't be
     * found for the given {@code typeReference} and {@code name}.
     *
     * @param typeReference the type of the component that could not be found, typically an interface
     * @param name          the name of the component that could not be found, potentially {@code null} when
     *                      unimportant
     */
    public ComponentNotFoundException(TypeReference<?> typeReference, @Nullable String name) {
        super(exceptionMessageFor(typeReference.getTypeAsClass(), name));
    }

    private static String exceptionMessageFor(Class<?> type, @Nullable String name) {
        return name != null
                ? "No component found for type [" + type + "] name [" + name + "]."
                : "No component found for type [" + type + "].";
    }
}
