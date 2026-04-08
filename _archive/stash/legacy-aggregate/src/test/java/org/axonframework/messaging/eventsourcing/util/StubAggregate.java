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

package org.axonframework.messaging.eventsourcing.util;

import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.AggregateLifecycle;
import org.axonframework.common.util.StubDomainEvent;

/**
 * @author Allard Buijze
 */
public class StubAggregate {

    @AggregateIdentifier
    private String identifier;

    public StubAggregate() {
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public StubAggregate(Object identifier) {
        this.identifier = identifier.toString();
    }

    public void doSomething() {
        AggregateLifecycle.apply(new StubDomainEvent());
    }

    public String getIdentifier() {
        return identifier;
    }

    @EventSourcingHandler
    protected void handle(EventMessage event) {
        identifier = ((DomainEventMessage) event).getAggregateIdentifier();
    }

    public void delete() {
        AggregateLifecycle.apply(new StubDomainEvent());
        AggregateLifecycle.markDeleted();
    }
}
