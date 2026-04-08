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

package org.axonframework.messaging.eventhandling;

import org.axonframework.messaging.core.QualifiedName;

/**
 * Exception thrown whenever an {@link EventHandlingComponent} is given an {@link EventMessage} for which it does not
 * have a {@link SimpleEventHandlingComponent#subscribe(QualifiedName, EventHandler) subscribed} {@link EventHandler}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class NoHandlerForEventException extends RuntimeException {

    /**
     * Constructs a {@code NoHandlerForEventException} signaling there was no {@link EventHandler} for the given
     * {@code eventName} present in the component with the given {@code componentName}.
     *
     * @param eventName     The qualified name for which there was no {@link EventHandler}.
     * @param componentName The name of the component that did not have an {@link EventHandler} for the given
     *                      {@code eventName}.
     */
    public NoHandlerForEventException(QualifiedName eventName, String componentName) {
        super("No handler found for event with name [" + eventName + "] in component [" + componentName + "]");
    }
}
