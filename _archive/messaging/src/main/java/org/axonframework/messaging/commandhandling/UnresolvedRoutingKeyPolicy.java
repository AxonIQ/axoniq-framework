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

package org.axonframework.messaging.commandhandling;

import org.axonframework.messaging.core.retry.RetryScheduler;

import java.util.function.Function;

import static java.lang.String.format;

/**
 * Set of simple {@link RoutingStrategy} implementations.
 * <p>
 * Could for example be used as fallback solutions when another {@code RoutingStrategy} is unable to resolve the routing
 * key.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public enum UnresolvedRoutingKeyPolicy implements RoutingStrategy {

    /**
     * A {@link RoutingStrategy} which always throws a {@link CommandDispatchException} regardless of the
     * {@link CommandMessage} received. Only feasible as a fallback solution which should straight out fail if the
     * intended policy is unable to resolve a routing key.
     * <p>
     * Note that when the routing key is based on static content in the {@code CommandMessage}, the exception raised
     * should extend from {@link org.axonframework.common.AxonNonTransientException} to indicate that retries do not
     * have a chance to succeed.
     *
     * @see RetryScheduler
     */
    ERROR(command -> {
        throw new CommandDispatchException(format(
                "The command [%s] does not contain a routing key.", command.type()
        ));
    }),

    /**
     * Policy that indicates a random key is to be returned when no Routing Key can be found for a Command Message. This
     * effectively means the Command Message is routed to a random segment.
     * <p>
     * Although not required to be fully random, implementations are required to return a different key for each
     * incoming command. Multiple invocations for the same command message may return the same value, but are not
     * required to do so.
     */
    RANDOM_KEY(command -> Double.toHexString(Math.random())),

    /**
     * Policy that indicates a fixed key ("unresolved") should be returned when no Routing Key can be found for a
     * Command Message. This effectively means all Command Messages with unresolved routing keys are routed to a the
     * same segment. The load of that segment may therefore not match the load factor.
     */
    STATIC_KEY(command -> "unresolved");

    private final Function<CommandMessage, String> routingKeyResolver;

    UnresolvedRoutingKeyPolicy(Function<CommandMessage, String> routingKeyResolver) {
        this.routingKeyResolver = routingKeyResolver;
    }

    @Override
    public String getRoutingKey(CommandMessage command) {
        return routingKeyResolver.apply(command);
    }
}