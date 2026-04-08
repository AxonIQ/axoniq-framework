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

package org.axonframework.common.property;


import org.axonframework.common.AxonConfigurationException;

/**
 * Exception indicating that a predefined property is not accessible. Generally, this means that the property does not
 * conform to the accessibility requirements of the accessor.
 *
 * @author Maxim Fedorov
 * @author Allard Buijze
 * @since 2.0
 */
public class PropertyAccessException extends AxonConfigurationException {

    /**
     * Initializes the PropertyAccessException with given {@code message} and {@code cause}.
     *
     * @param message The message describing the cause
     * @param cause   The underlying cause of the exception
     */
    public PropertyAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
