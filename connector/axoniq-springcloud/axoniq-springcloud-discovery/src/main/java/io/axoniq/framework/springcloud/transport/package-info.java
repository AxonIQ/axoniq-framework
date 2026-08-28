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
 * The HTTP transport carrying commands and queries between members of the cluster.
 * <p>
 * A command is one request answered by one reply: {@link
 * io.axoniq.framework.springcloud.transport.CommandDispatchRequest} and {@link
 * io.axoniq.framework.springcloud.transport.CommandDispatchReply} are its wire format, {@link
 * io.axoniq.framework.springcloud.transport.RemoteCommandDispatcher} sends it, {@link
 * io.axoniq.framework.springcloud.transport.SpringCloudCommandController} receives it, and {@link
 * io.axoniq.framework.springcloud.transport.IncomingCommandGateway} turns a received request into an invocation of
 * the local handler and its result back into a reply.
 * <p>
 * A query may be answered any number of times, so it is one request answered by a stream of Server-Sent Events:
 * {@link io.axoniq.framework.springcloud.transport.QueryDispatchRequest} carries it out, {@link
 * io.axoniq.framework.springcloud.transport.QueryResponseEvent} and {@link
 * io.axoniq.framework.springcloud.transport.QueryErrorEvent} carry the answer back, and {@link
 * io.axoniq.framework.springcloud.transport.ServerSentEventReader} reads them without a reactive stack.
 */
@NullMarked
package io.axoniq.framework.springcloud.transport;

import org.jspecify.annotations.NullMarked;
