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

package org.axonframework.springboot.autoconfig.context;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.spring.stereotype.Aggregate;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

@Aggregate
public abstract class Animal {

    @AggregateIdentifier
    protected String aggregateId;
    protected String name;

    @CommandHandler
    public void handle(RenameAnimalCommand command) {
        apply(new AnimalRenamedEvent(command.getAggregateId(), command.getRename()));
    }

    @EventSourcingHandler
    public void on(AnimalRenamedEvent event) {
        this.name = event.getRename();
    }

    public Animal() {
    }
}
