/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.deadline;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.ScopeAware;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Translates a fired {@link DeadlineMessage} scoped to an {@link AggregateScopeDescriptor} into a command, dispatched
 * through a {@link CommandGateway}.
 * <p>
 * Implements {@link ScopeAware} so it can be reached through a {@code ScopeAwareProvider} the same way a Saga manager
 * is. Its {@link #send(Message, ProcessingContext, ScopeDescriptor)} dispatches the deadline's payload and metadata as
 * a command named after the deadline's {@link DeadlineMessage#getDeadlineName()}, and writes the
 * {@link AggregateScopeDescriptor}'s identifier into the dispatched command's metadata under
 * {@value AggregateDeadlineEntityIdResolverDefinition#DESCRIPTOR_BASED_ID}.
 * {@link AggregateDeadlineEntityIdResolverConfigurationEnhancer} registers the resolver that reads this metadata entry
 * as the application-wide default, used whenever the command's payload carries no {@code @TargetEntityId}.
 * <p>
 * Naming the command after the deadline keeps deadlines apart that share a payload type, such as two deadlines
 * carrying a {@code UUID}. The migrated handler of a deadline with a payload becomes
 * {@code @CommandHandler(commandName = "<deadlineName>") void handle(<Payload> payload, ...)}. A deadline scheduled
 * without a payload carries {@code null} as its {@link Message#payload()}, and is dispatched with the deadline name as
 * its payload instead, so its handler becomes
 * {@code @CommandHandler(commandName = "<deadlineName>") void handle(String deadlineName, ...)}.
 * <p>
 * Handlers migrated with an earlier version of this translator are named after the deadline's payload type. To keep
 * them working, a deadline with a payload that finds no handler for its deadline name is dispatched again, named after
 * its payload type, and a warning asks to rename the handler.
 * <p>
 * Constructed and registered by the {@link AggregateDeadlineCommandTranslatorConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 * @see AggregateDeadlineCommandTranslatorConfigurationEnhancer
 * @see AggregateDeadlineEntityIdResolverConfigurationEnhancer
 * @see AggregateDeadlineEntityIdResolverDefinition
 * @since 5.4.0
 */
@Internal
public class AggregateDeadlineCommandTranslator implements ScopeAware {

    private static final Logger logger = LoggerFactory.getLogger(AggregateDeadlineCommandTranslator.class);

    private final CommandGateway commandGateway;

    /**
     * Initializes an {@code AggregateDeadlineCommandTranslator}, dispatching translated commands through the given
     * {@code commandGateway}.
     *
     * @param commandGateway the {@link CommandGateway} to dispatch a fired aggregate deadline's command through
     */
    public AggregateDeadlineCommandTranslator(CommandGateway commandGateway) {
        this.commandGateway = Objects.requireNonNull(commandGateway, "The CommandGateway may not be null.");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns {@code true} for every {@link AggregateScopeDescriptor}, regardless of the aggregate type it describes,
     * since routing to the right entity is delegated to the dispatched command's {@code @TargetEntityId}, not resolved
     * here.
     */
    @Override
    public boolean canResolve(ScopeDescriptor scopeDescription) {
        return scopeDescription instanceof AggregateScopeDescriptor;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Dispatches the {@code message}'s payload and metadata as a command named after
     * {@link DeadlineMessage#getDeadlineName()} through the configured {@link CommandGateway}, passing the given
     * {@code context} along so the command runs within the deadline's firing unit of work. Waits for the command's
     * result with {@link FutureUtils#joinAndUnwrap(java.util.concurrent.CompletableFuture)}, rethrowing a failure so the
     * caller applies its own failure handling.
     * <p>
     * When the command bus has no handler for the deadline name, a {@code message} with a payload is dispatched again
     * as its payload, named after the payload's type, for handlers migrated with an earlier version of this translator.
     * <p>
     * A {@code message} whose payload is an {@link UnknownDeadlinePayload} (its stored type could not be resolved) is
     * not dispatched: this method logs a warning naming the deadline, the stored type and the aggregate scope, and
     * returns.
     * <p>
     * A {@code message} whose payload is {@code null} is dispatched with {@link DeadlineMessage#getDeadlineName()} as
     * its payload, without the fallback.
     * <p>
     * The dispatched command's metadata always carries the {@code scopeDescription}'s identifier under
     * {@value AggregateDeadlineEntityIdResolverDefinition#DESCRIPTOR_BASED_ID}, so the target entity can be resolved
     * from it instead of from the payload.
     *
     * @throws IllegalArgumentException if the given {@code message} is not a {@link DeadlineMessage}, or if the given
     *                                  {@code scopeDescription} is not an {@link AggregateScopeDescriptor}
     */
    @Override
    public void send(
            Message message,
            ProcessingContext context,
            ScopeDescriptor scopeDescription
    ) throws Exception {
        if (!(message instanceof DeadlineMessage deadlineMessage)) {
            throw new IllegalArgumentException(String.format(
                    "Expected a DeadlineMessage for aggregate scope [%s], but got [%s].",
                    scopeDescription.scopeDescription(), message.getClass().getName()
            ));
        }
        if (!(scopeDescription instanceof AggregateScopeDescriptor aggregateScope)) {
            throw new IllegalArgumentException(String.format(
                    "Expected an AggregateScopeDescriptor for deadline [%s], but got [%s].",
                    deadlineMessage.getDeadlineName(), scopeDescription.getClass().getName()
            ));
        }

        Object payload = deadlineMessage.payload();
        if (payload instanceof UnknownDeadlinePayload unknownPayload) {
            logger.warn(
                    "Deadline [{}] fired with an unknown payload type [{}] for aggregate scope [{}]. "
                            + "The deadline is not dispatched as a command.",
                    deadlineMessage.getDeadlineName(), unknownPayload.typeName(), aggregateScope.scopeDescription()
            );
            return;
        }

        Metadata metadata = deadlineMessage.metadata().and(
                AggregateDeadlineEntityIdResolverDefinition.DESCRIPTOR_BASED_ID,
                String.valueOf(aggregateScope.getIdentifier())
        );
        String deadlineName = deadlineMessage.getDeadlineName();
        CommandMessage command = new GenericCommandMessage(
                new MessageType(deadlineName), payload == null ? deadlineName : payload, metadata
        );
        try {
            FutureUtils.joinAndUnwrap(commandGateway.send(command, context).getResultMessage());
        } catch (NoHandlerForCommandException e) {
            if (payload == null) {
                throw e;
            }
            logger.warn(
                    "No command handler found for deadline [{}]. Dispatching it as a command named after its payload "
                            + "type [{}] instead. Rename that handler to @CommandHandler(commandName = \"{}\"), "
                            + "as deadlines sharing a payload type cannot be told apart otherwise.",
                    deadlineName, payload.getClass().getName(), deadlineName
            );
            FutureUtils.joinAndUnwrap(commandGateway.send(payload, metadata, context).getResultMessage());
        }
    }
}
