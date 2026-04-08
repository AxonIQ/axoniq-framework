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

/**
 * An exception indicating a {@link Component} is being {@link Configuration#getComponent(Class, String) retrieved} for
 * a type and name combination that resulted in several matches.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class AmbiguousComponentMatchException extends RuntimeException {

    /**
     * Constructs an {@code AmbiguousComponentMatchException} for the given {@code identifier}.
     *
     * @param identifier The identifier for which to create an {@code AmbiguousComponentMatchException}.
     * @param <C>        The {@link Component.Identifier#type()} of the given {@code identifier}.
     */
    public <C> AmbiguousComponentMatchException(Component.Identifier<C> identifier) {
        super("No single instance found for type ["
                      + identifier.typeAsClass()
                      + "] and name ["
                      + identifier.name()
                      + "]. Please try a more specific type-name combination to retrieve components.");
    }
}
