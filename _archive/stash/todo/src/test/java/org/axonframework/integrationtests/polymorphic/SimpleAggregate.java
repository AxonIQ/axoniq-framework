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

package org.axonframework.integrationtests.polymorphic;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.CreationPolicy;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;
import static org.axonframework.modelling.command.AggregateLifecycle.createNew;

/**
 * Non-polymorphic aggregate that creates a polymorphic aggregate.
 *
 * @author Milan Savic
 */
@Entity
public class SimpleAggregate {

    @Id
    private String id;

    public SimpleAggregate() {
    }

    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.ALWAYS)
    public void handle(CreateSimpleAggregateCommand cmd) throws Exception {
        apply(new SimpleAggregateCreatedEvent(cmd.getId()));
        createNew(ParentAggregate.class, () -> {
            Child1Aggregate child1Aggregate = new Child1Aggregate();
            child1Aggregate.handle(new CreateChild1Command("child1" + cmd.getId()));
            return child1Aggregate;
        });
    }

    @EventSourcingHandler
    public void on(SimpleAggregateCreatedEvent evt) {
        this.id = evt.getId();
    }
}
