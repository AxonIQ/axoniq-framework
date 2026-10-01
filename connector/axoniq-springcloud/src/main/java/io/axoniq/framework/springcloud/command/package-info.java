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

/**
 * The command side of the Spring Cloud connector: the connector distributing commands, and the HTTP transport carrying
 * them between members of the cluster.
 * <p>
 * {@link io.axoniq.framework.springcloud.command.SpringCloudCommandBusConnector} picks the member owning a command's
 * routing key and hands the command to the transport. A command is one request answered by one reply: {@link
 * io.axoniq.framework.springcloud.command.CommandDispatchRequest} and {@link
 * io.axoniq.framework.springcloud.command.CommandDispatchReply} are its wire format, {@link
 * io.axoniq.framework.springcloud.command.RemoteCommandDispatcher} sends it, {@link
 * io.axoniq.framework.springcloud.command.SpringCloudCommandController} receives it, and {@link
 * io.axoniq.framework.springcloud.command.IncomingCommandInvoker} turns a received request into an invocation of
 * the local handler and its result back into a reply.
 */
@NullMarked
package io.axoniq.framework.springcloud.command;

import org.jspecify.annotations.NullMarked;
