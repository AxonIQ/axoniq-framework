/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.command;

import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Sends a command to another member of the cluster and reports back the outcome of handling it there.
 * <p>
 * The distinction this contract has to preserve is between a member that could not be reached and a handler that ran
 * and failed. Only the former says anything about the member's availability, and the connector uses that to decide
 * whether to take the member out of its routing ring, so an unreachable member must be reported as a
 * {@link CommandDispatchException} while a failed handler must not.
 * <p>
 * {@link HttpRemoteCommandDispatcher} is the implementation used in practice. This contract is separate from it so
 * that the connector's routing can be exercised without a web stack, and so that a different transport can be put
 * underneath without touching the connector.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public interface RemoteCommandDispatcher {

    /**
     * Sends the given {@code command} to the given {@code member} and completes with the outcome of handling it there.
     *
     * @param member  the member to send the command to. Never {@link Member#local() local}
     * @param command the command to send
     * @return a future completing with the result of handling the command on the given {@code member}, with
     * {@code null} when its handler returned none; completing exceptionally with a
     * {@link CommandDispatchException} when the member could not be reached, and with the handler's own failure when
     * it ran and failed
     */
    CompletableFuture<@Nullable CommandResultMessage> dispatch(Member member, CommandMessage command);
}
