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
 * The HTTP transport carrying commands between members of the cluster.
 * <p>
 * {@link io.axoniq.framework.springcloud.transport.CommandDispatchRequest} and {@link
 * io.axoniq.framework.springcloud.transport.CommandDispatchReply} are the wire format. {@link
 * io.axoniq.framework.springcloud.transport.RemoteCommandDispatcher} sends a command to another member, {@link
 * io.axoniq.framework.springcloud.transport.SpringCloudCommandController} receives one, and {@link
 * io.axoniq.framework.springcloud.transport.IncomingCommandInvoker} turns a received request into an invocation of
 * the local command handler and its result back into a reply.
 */
@NullMarked
package io.axoniq.framework.springcloud.transport;

import org.jspecify.annotations.NullMarked;
