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

package org.axonframework.modelling.command;

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception indicating that the an aggregate could not be found in the repository.
 *
 * @author Allard Buijze
 * @since 0.4
 */
public class AggregateNotFoundException extends AxonNonTransientException {

    private final String aggregateIdentifier;

    /**
     * Initialize a AggregateNotFoundException for an aggregate identifier by given {@code aggregateIdentifier}
     * and given {@code message}.
     *
     * @param aggregateIdentifier The identifier of the aggregate that could not be found
     * @param message             The message describing the cause of the exception
     */
    public AggregateNotFoundException(String aggregateIdentifier, String message) {
        super(message);
        this.aggregateIdentifier = aggregateIdentifier;
    }

    /**
     * Initialize a AggregateNotFoundException for an aggregate identifier by given {@code aggregateIdentifier}
     * and
     * with the given {@code message} and {@code cause}.
     *
     * @param aggregateIdentifier The identifier of the aggregate that could not be found
     * @param message             The message describing the cause of the exception
     * @param cause               The underlying cause of the exception
     */
    public AggregateNotFoundException(String aggregateIdentifier, String message, Throwable cause) {
        super(message, cause);
        this.aggregateIdentifier = aggregateIdentifier;
    }

    /**
     * Returns the identifier of the aggregate that could not be found.
     *
     * @return the identifier of the aggregate that could not be found
     */
    public Object getAggregateIdentifier() {
        return aggregateIdentifier;
    }
}
