/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * The command side of the Spring Cloud connector: the connector distributing commands, and the HTTP transport carrying
 * them between members of the cluster.
 * <p>
 * {@link SpringCloudCommandBusConnector} picks the member owning a command's routing key, and either invokes this
 * application's own handler when that member is the local one, or hands the command to the transport. A command is one
 * request answered by one reply: {@link CommandDispatchRequest} and {@link CommandDispatchReply} are its wire format,
 * {@link RemoteCommandDispatcher} sends it, {@link SpringCloudCommandController} receives it, and
 * {@link IncomingCommandInvoker} turns a received request into an invocation of the local handler and its result back
 * into a reply.
 */
@NullMarked
package io.axoniq.framework.springcloud.command;

import org.jspecify.annotations.NullMarked;
