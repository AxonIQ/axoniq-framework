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
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.integrationtests.testsuite.administration.commands.GiveRaise;
import org.axonframework.integrationtests.testsuite.administration.events.RaiseGiven;

public class MutableSalaryInformation {
    private Double salary;
    private String role;

    public MutableSalaryInformation(Double salary, String role) {
        this.salary = salary;
        this.role = role;
    }

    @CommandHandler
    public void handle(GiveRaise command, EventAppender appender) {
        if (command.newSalary() <= salary) {
            throw new IllegalStateException("New salary must be greater than current salary");
        }
        appender.append(new RaiseGiven(command.identifier(), command.newSalary()));
    }

    @EventSourcingHandler
    public void on(RaiseGiven event) {
        this.salary = event.newSalary();
    }
}
