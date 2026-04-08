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

package org.axonframework.messaging.commandhandling.gateway;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;

/**
 * A {@link CommandDispatcher} that publishes commands to a {@link CommandGateway} in a predefined
 * {@link ProcessingContext}.
 * <p>
 * Any commands dispatched through this {@code CommandDispatcher} occur within the {@code context} this dispatcher was
 * created with. You can construct one through the
 * {@link CommandDispatcher#forContext(ProcessingContext)} method.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public class ContextAwareCommandDispatcher implements CommandDispatcher {

    private final CommandGateway commandGateway;
    private final ProcessingContext context;

    ContextAwareCommandDispatcher(CommandGateway commandGateway,
                                  ProcessingContext context) {
        this.commandGateway = Objects.requireNonNull(commandGateway, "The Command Gateway must not be null.");
        this.context = Objects.requireNonNull(context, "The Processing Context must not be null.");
    }

    @Override
    public CommandResult send(Object command) {
        return commandGateway.send(command, context);
    }

    @Override
    public CommandResult send(Object command, Metadata metadata) {
        return commandGateway.send(command, metadata, context);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("processingContext", context);
        descriptor.describeProperty("commandGateway", commandGateway);
    }
}
