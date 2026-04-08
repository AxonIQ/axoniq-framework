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

package org.axonframework.integrationtests.commandhandling;

import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * @author Allard Buijze
 */
public class StubAggregate {

    private int changeCounter;

    @AggregateIdentifier
    private String identifier;

    public StubAggregate(String aggregateIdentifier) {
        identifier = aggregateIdentifier;
    }

    public void handle(Object aggregateId) {
        apply(new StubAggregateCreatedEvent(aggregateId));
    }

    public StubAggregate() {
    }

    public void makeAChange() {
        apply(new StubAggregateChangedEvent());
    }

    public void causeTrouble() {
        throw new RuntimeException("That's problematic");
    }

    @EventSourcingHandler
    private void onCreated(StubAggregateCreatedEvent event) {
        this.identifier = event.getAggregateIdentifier().toString();
        changeCounter = 0;
    }

    @EventSourcingHandler
    private void onChange(StubAggregateChangedEvent event) {
        changeCounter++;
    }

    public void makeALoopingChange() {
        apply(new LoopingChangeDoneEvent(identifier));
    }
}
