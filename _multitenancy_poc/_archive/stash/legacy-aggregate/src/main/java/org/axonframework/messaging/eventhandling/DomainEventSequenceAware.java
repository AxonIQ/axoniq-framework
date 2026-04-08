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

import java.util.Optional;

/**
 * Contract describing a component which is aware of {@link DomainEventMessage} their sequences and is capable of
 * providing the last known sequence number for a given Aggregate identifier.
 *
 * @author Steven van Beelen
 * @since 4.0
 */
public interface DomainEventSequenceAware {

    /**
     * Returns the last known sequence number of an Event for the given {@code aggregateIdentifier}.
     * <p>
     * It is preferred to retrieve the last known sequence number from the Domain Event Stream when sourcing an
     * Aggregate from events. However, this method provides an alternative in cases no events have been read. For
     * example when using state storage.
     *
     * @param aggregateIdentifier the identifier of the aggregate to find the highest sequence for
     * @return an optional containing the highest sequence number found, or an empty optional is no events are found
     * for this aggregate
     */
    Optional<Long> lastSequenceNumberFor(String aggregateIdentifier);
}
