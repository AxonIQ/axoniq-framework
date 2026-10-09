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
 * a command, and writes the {@link AggregateScopeDescriptor}'s identifier into the dispatched command's metadata under
 * {@value AggregateDeadlineEntityIdResolverDefinition#DESCRIPTOR_BASED_ID}.
 * {@link AggregateDeadlineEntityIdResolverConfigurationEnhancer} registers the resolver that reads this metadata entry
 * as the application-wide default, used whenever the command's payload carries no {@code @TargetEntityId}.
 * <p>
 * A deadline scheduled without a payload carries {@code null} as its {@link Message#payload()}. Since a {@code null}
 * payload cannot name the command to dispatch, this translator falls back to the deadline's
 * {@link DeadlineMessage#getDeadlineName()} in that case, dispatched as both the command's name and its payload, so the
 * migrated handler becomes {@code @CommandHandler(commandName = "<deadlineName>") void handle(String command, ...)}.
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
     * Dispatches the {@code message}'s payload and metadata as a command through the configured {@link CommandGateway},
     * passing the given {@code context} along so the command runs within the deadline's firing unit of work. Waits for
     * the command's result with {@link FutureUtils#joinAndUnwrap(java.util.concurrent.CompletableFuture)}, rethrowing a
     * failure so the caller applies its own failure handling.
     * <p>
     * A {@code message} whose payload is an {@link UnknownDeadlinePayload} (its stored type could not be resolved) is
     * not dispatched: this method logs a warning naming the deadline, the stored type and the aggregate scope, and
     * returns.
     * <p>
     * A {@code message} whose payload is {@code null} is dispatched as a {@link GenericCommandMessage} named after, and
     * carrying as its payload, {@link DeadlineMessage#getDeadlineName()}, since a {@code null} payload carries no type
     * to derive a command name from.
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
        if (payload == null) {
            String deadlineName = deadlineMessage.getDeadlineName();
            CommandMessage command = new GenericCommandMessage(new MessageType(deadlineName), deadlineName, metadata);
            FutureUtils.joinAndUnwrap(commandGateway.send(command, context).getResultMessage());
        } else {
            FutureUtils.joinAndUnwrap(commandGateway.send(payload, metadata, context).getResultMessage());
        }
    }
}
