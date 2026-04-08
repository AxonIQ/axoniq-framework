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
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.CreationPolicy;
import org.axonframework.spring.stereotype.Aggregate;

import java.util.UUID;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

@Aggregate
public class Cat extends Animal {

    // Field present to have the Dog aggregate differ in structure from the Cat for snapshot testing.
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private String hairColor;

    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.ALWAYS)
    public void handle(CreateCatCommand command) {
        apply(new CatCreatedEvent(command.getAggregateId(), command.getName()));
    }

    @EventSourcingHandler
    public void on(CatCreatedEvent event) {
        this.aggregateId = event.getAggregateId();
        this.name = event.getName();
        this.hairColor = UUID.randomUUID().toString();
    }

    public Cat() {
        // Required by Axon
    }
}
