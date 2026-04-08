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

package org.axonframework.modelling.command.inspection;

import org.axonframework.common.AxonConfigurationException;

/**
 * Thrown if an aggregate model is invalid.
 *
 * @author Milan Savic
 * @since 4.3
 */
public class AggregateModellingException extends AxonConfigurationException {

    /**
     * Constructs this exception with given {@code message} explaining the cause.
     *
     * @param message The message explaining the cause
     */
    public AggregateModellingException(String message) {
        super(message);
    }
}
