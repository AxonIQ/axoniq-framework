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

package org.axonframework.spring.authorization;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.CreationPolicy;
import org.axonframework.extension.spring.stereotype.EventSourced;
import org.springframework.security.access.annotation.Secured;

import java.util.UUID;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * Test Aggregate
 *
 * @author Roald Bankras
 */
@EventSourced(idType = UUID.class)
public class TestAggregate {

    @AggregateIdentifier
    private UUID aggregateId;

    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.ALWAYS)
    @Secured("ROLE_aggregate.create")
    public void create(CreateAggregateCommand cmd) {
        apply(new AggregateCreatedEvent(cmd.getAggregateId()));
    }

    @CommandHandler
    public void update(UpdateAggregateCommand cmd) {
        apply(new AggregateUpdatedEvent(cmd.getAggregateId()));
    }

    @EventSourcingHandler
    public void on(AggregateCreatedEvent evt) {
        aggregateId = evt.getAggregateId();
    }
}

