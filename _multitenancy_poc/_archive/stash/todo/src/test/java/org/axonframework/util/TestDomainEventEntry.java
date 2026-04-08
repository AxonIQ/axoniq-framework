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

package org.axonframework.util;

import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import org.axonframework.messaging.eventhandling.AbstractSequencedDomainEventEntry;
import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.conversion.Serializer;

/**
 * Stub {@link AbstractSequencedDomainEventEntry}, used for testing purposes.
 *
 * @author Steven van Beelen
 */
@Entity
@Table(indexes = @Index(columnList = "aggregateIdentifier,sequenceNumber,type", unique = true))
public class TestDomainEventEntry extends AbstractSequencedDomainEventEntry<String> {

    public TestDomainEventEntry(DomainEventMessage event, Serializer serializer) {
        super(event, serializer, String.class);
    }

    @SuppressWarnings("unused")
    protected TestDomainEventEntry() {
    }
}
