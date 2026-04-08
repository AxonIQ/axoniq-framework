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
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.CommandHandlerInterceptor;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * The abstract parent aggregate in this polymorphic aggregate hierarchy. It represents the type of the aggregate as a
 * whole.
 *
 * @author Milan Savic
 */
@Entity
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class ParentAggregate {

    @AggregateIdentifier
    @Id
    protected String id;

    protected String state;

    public String getState() {
        return state;
    }

    @CommandHandler
    public String handle(CommonCommand cmd) {
        return this.getClass().getSimpleName() + cmd.getId();
    }

    @CommandHandler
    public static ParentAggregate create(CreateChildFactoryCommand cmd) {
        if (cmd.getChild() == 1) {
            Child1Aggregate child1Aggregate = new Child1Aggregate();
            child1Aggregate.handle(new CreateChild1Command(cmd.getId()));
            return child1Aggregate;
        } else {
            Child2Aggregate child2Aggregate = new Child2Aggregate();
            child2Aggregate.handle(new CreateChild2Command(cmd.getId()));
            return child2Aggregate;
        }
    }

    @EventHandler
    public void on(ParentEvent evt) {
        this.state = "parent" + evt.getId();
    }

    @CommandHandler
    public void handle(FireChildEventCommand cmd) {
        apply(new ChildEvent(cmd.getId()));
    }

    @CommandHandlerInterceptor
    public void intercept(InterceptedByParentCommand cmd) {
        cmd.setState(cmd.getState() + "InterceptedByParent");
    }

    @CommandHandler
    public String handle(InterceptedByChildCommand cmd) {
        return cmd.getState() + "HandledByParent";
    }

    @CommandHandler
    public abstract String handle(AbstractCommandHandlerCommand cmd);
}
