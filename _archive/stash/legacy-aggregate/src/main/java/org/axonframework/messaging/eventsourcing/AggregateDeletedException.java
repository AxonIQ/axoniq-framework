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

package org.axonframework.messaging.eventsourcing;

import org.axonframework.modelling.command.AggregateNotFoundException;

/**
 * Special case of the {@link AggregateNotFoundException} that indicates that historic
 * information of an aggregate was found, but the aggregate has been deleted.
 *
 * @author Allard Buijze
 * @since 0.4
 */
public class AggregateDeletedException extends AggregateNotFoundException {

    /**
     * Initialize a AggregateDeletedException for an aggregate identifier by given {@code aggregateIdentifier} and
     * given {@code message}.
     *
     * @param aggregateIdentifier The identifier of the aggregate that has been deleted
     * @param message             The message describing the cause of the exception
     */
    public AggregateDeletedException(String aggregateIdentifier, String message) {
        super(aggregateIdentifier, message);
    }

    /**
     * Initialize a AggregateDeletedException for an aggregate identifier by given {@code aggregateIdentifier} and
     * a default {@code message}.
     *
     * @param aggregateIdentifier The identifier of the aggregate that has been deleted
     */
    public AggregateDeletedException(String aggregateIdentifier) {
        this(aggregateIdentifier,
             String.format("Aggregate with identifier [%s] not found. It has been deleted.", aggregateIdentifier));
    }
}
