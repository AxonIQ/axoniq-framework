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

package io.axoniq.framework.integrationtests.testsuite.administration;

import org.axonframework.integrationtests.testsuite.administration.commands.AssignTaskCommand;
import org.axonframework.integrationtests.testsuite.administration.commands.ChangeEmailAddress;
import org.axonframework.integrationtests.testsuite.administration.commands.CompleteTaskCommand;
import org.axonframework.integrationtests.testsuite.administration.commands.CreateCustomer;
import org.axonframework.integrationtests.testsuite.administration.commands.CreateEmployee;
import org.axonframework.integrationtests.testsuite.administration.commands.GiveRaise;
import org.axonframework.integrationtests.testsuite.administration.commands.PersonCommand;
import org.axonframework.integrationtests.testsuite.administration.common.PersonIdentifier;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.EntityIdResolver;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;

class PersonIdentifierEntityIdResolver implements EntityIdResolver<PersonIdentifier> {

    @NonNull
    @Override
    public PersonIdentifier resolve(@NonNull Message message, @NonNull ProcessingContext context) {
        List<Class<? extends PersonCommand>> personCommandTypes = List.of(
                AssignTaskCommand.class,
                CreateCustomer.class,
                CreateEmployee.class,
                ChangeEmailAddress.class,
                CompleteTaskCommand.class,
                GiveRaise.class
        );
        var clazz = personCommandTypes.stream()
                                      .filter(type -> type.getName().equals(message.type().name()))
                                      .findFirst()
                                      .orElseThrow(() -> new IllegalArgumentException(format(
                                              "Unknown command type: %s",
                                              message.type().name()
                                      )));
        return Objects.requireNonNull(message.payloadAs(clazz)).identifier();
    }
}