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

import java.util.Random;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

@Aggregate
public class Dog extends Animal {

    // Field present to have the Cat aggregate differ in structure from the Dog for snapshot testing.
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private int ageInYears;

    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.ALWAYS)
    public void handle(CreateDogCommand command) {
        apply(new DogCreatedEvent(command.getAggregateId(), command.getName()));
    }

    @EventSourcingHandler
    public void on(DogCreatedEvent event) {
        this.aggregateId = event.getAggregateId();
        this.name = event.getName();
        this.ageInYears = new Random().nextInt(7);
    }

    public Dog() {
        // Required by Axon
    }
}
