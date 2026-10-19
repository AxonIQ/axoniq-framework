/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
import org.axonframework.messaging.commandhandling.gateway.CommandResult;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
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
 * a command named {@code <aggregateType>.<deadlineName>}: a {@link QualifiedName} with the
 * {@link AggregateScopeDescriptor#getType() aggregate type} as namespace and the deadline's
 * {@link DeadlineMessage#getDeadlineName()} as local name. It writes the {@link AggregateScopeDescriptor}'s identifier
 * into the dispatched command's metadata under
 * {@value AggregateDeadlineEntityIdResolverDefinition#DESCRIPTOR_BASED_ID}.
 * {@link AggregateDeadlineEntityIdResolverConfigurationEnhancer} registers the resolver that reads this metadata entry
 * as the application-wide default, used whenever the command's payload carries no {@code @TargetEntityId}.
 * <p>
 * Naming the command after the aggregate type and the deadline keeps deadlines apart that share a payload type, such
 * as two deadlines carrying a {@code UUID}, and deadlines of different aggregate types that share a name. The aggregate
 * type is the one Axon Framework 4 stored with the deadline: the aggregate's simple class name, unless its
 * {@code @Aggregate} annotation set another type. The migrated handler of a deadline with a payload becomes
 * {@code @CommandHandler(commandName = "<aggregateType>.<deadlineName>") void handle(<Payload> payload, ...)}. A
 * deadline scheduled without a payload carries {@code null} as its {@link Message#payload()}, and is dispatched with
 * the deadline name as its payload instead, so its handler becomes
 * {@code @CommandHandler(commandName = "<aggregateType>.<deadlineName>") void handle(String deadlineName, ...)}.
 * <p>
 * Handlers migrated with an earlier version of this translator are named after the deadline's payload type, or after
 * the deadline name alone for a deadline without a payload. To keep them working, a deadline that finds no handler for
 * its qualified name is dispatched again under that earlier name, and a warning asks to rename the handler.
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
     * Dispatches the {@code message}'s payload and metadata as a command named after the aggregate type of the
     * {@code scopeDescription} and {@link DeadlineMessage#getDeadlineName()}, through the configured
     * {@link CommandGateway}, passing the given
     * {@code context} along so the command runs within the deadline's firing unit of work. Waits for the command's
     * result with {@link FutureUtils#joinAndUnwrap(java.util.concurrent.CompletableFuture)}, rethrowing a failure so the
     * caller applies its own failure handling.
     * <p>
     * When the command bus has no handler for that name, the {@code message} is dispatched again under the name an
     * earlier version of this translator used, for handlers migrated with it: named after the payload's type, or after
     * the deadline name alone for a {@code message} without a payload.
     * <p>
     * A {@code message} whose payload is an {@link UnknownDeadlinePayload} (its stored type could not be resolved) is
     * not dispatched: this method logs a warning naming the deadline, the stored type and the aggregate scope, and
     * returns.
     * <p>
     * A {@code message} whose payload is {@code null} is dispatched with {@link DeadlineMessage#getDeadlineName()} as
     * its payload.
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
        QualifiedName commandName = new QualifiedName(aggregateScope.getType(), deadlineName);
        Object commandPayload = payload == null ? deadlineName : payload;
        CommandMessage command = new GenericCommandMessage(new MessageType(commandName), commandPayload, metadata);
        try {
            FutureUtils.joinAndUnwrap(commandGateway.send(command, context).getResultMessage());
        } catch (NoHandlerForCommandException e) {
            logger.warn(
                    "No command handler found for [{}]. Dispatching deadline [{}] under the name of its earlier "
                            + "handler [{}] instead. Rename that handler to @CommandHandler(commandName = \"{}\"), "
                            + "as deadlines sharing a payload type or a deadline name cannot be told apart otherwise.",
                    commandName, deadlineName, payload == null ? deadlineName : payload.getClass().getName(),
                    commandName
            );
            // Handlers migrated with 5.4.0 are named after the payload type, or the deadline name without a payload.
            CommandResult fallback;
            if (payload == null) {
                CommandMessage earlierCommand =
                        new GenericCommandMessage(new MessageType(deadlineName), deadlineName, metadata);
                fallback = commandGateway.send(earlierCommand, context);
            } else {
                fallback = commandGateway.send(payload, metadata, context);
            }
            FutureUtils.joinAndUnwrap(fallback.getResultMessage());
        }
    }
}
