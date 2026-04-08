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

/**
 * A {@link RuntimeException} thrown whenever a {@link Component} has been overridden in a {@link ComponentRegistry}.
 * <p>
 * Is typically only thrown whenever the {@link OverridePolicy} is set to {@link OverridePolicy#REJECT}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class ComponentOverrideException extends RuntimeException {

    /**
     * Constructs a {@code ComponentOverrideException} with the given {@code type} and {@code name} as the unique
     * identifier of the {@link Component} that has been overridden in a {@link ApplicationConfigurer}.
     *
     * @param type The type of the component this object identifiers, typically an interface.
     * @param name The name of the component this object identifiers, potentially {@code null} when unimportant.
     */
    public ComponentOverrideException(Class<?> type, @Nullable String name) {
        super(exceptionMessageFor(type, name));
    }

    private static String exceptionMessageFor(Class<?> type, @Nullable String name) {
        if (name != null) {
            return "Cannot override Component with type [" + type + "] and name ["
                    + name + "]; it is already registered.";
        }
        return "Cannot override Component with type [" + type + "]; it is already registered. "
                + "To allow multiple components with the same type a unique name is required.";
    }
}
