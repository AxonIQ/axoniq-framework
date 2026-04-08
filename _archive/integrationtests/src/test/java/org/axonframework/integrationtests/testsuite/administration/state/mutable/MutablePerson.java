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

package org.axonframework.integrationtests.testsuite.administration.state.mutable;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.eventsourcing.annotation.EventCriteriaBuilder;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.annotation.reflection.InjectEntityId;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.integrationtests.testsuite.administration.commands.ChangeEmailAddress;
import org.axonframework.integrationtests.testsuite.administration.common.PersonIdentifier;
import org.axonframework.integrationtests.testsuite.administration.common.PersonType;
import org.axonframework.integrationtests.testsuite.administration.events.EmailAddressChanged;

@EventSourcedEntity(
        concreteTypes = {
                MutableEmployee.class,
                MutableCustomer.class
        }
)
public abstract class MutablePerson {

    protected PersonIdentifier identifier;
    protected String emailAddress;

    @CommandHandler
    public void handle(ChangeEmailAddress command, EventAppender appender) {
        if (command.emailAddress() == null || command.emailAddress().isBlank()) {
            throw new IllegalArgumentException("Email address cannot be null or blank");
        }
        if (command.emailAddress().equals(emailAddress)) {
            throw new IllegalArgumentException("Email address cannot be the same as the current one");
        }

        appender.append(new EmailAddressChanged(command.identifier(), command.emailAddress()));
    }

    @EventSourcingHandler
    public void on(EmailAddressChanged event) {
        this.emailAddress = event.emailAddress();
    }

    @EntityCreator
    public static MutablePerson create(@InjectEntityId PersonIdentifier id) {
        if (id.type() == PersonType.EMPLOYEE) {
            return new MutableEmployee();
        } else if (id.type() == PersonType.CUSTOMER) {
            return new MutableCustomer();
        }
        throw new IllegalArgumentException("Unknown type: " + id.type());
    }

    @EventCriteriaBuilder
    static EventCriteria eventCriteria(PersonIdentifier identifier) {
        return EventCriteria.havingTags("Person", identifier.key());
    }
}
