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
 * The query side of the Spring Cloud connector: the connector distributing queries and subscription queries, and the
 * HTTP transport carrying them between members of the cluster.
 * <p>
 * {@link io.axoniq.framework.springcloud.query.SpringCloudQueryBusConnector} rotates each query over the members
 * advertising its name and hands it to the transport. A query may be answered any number of times, so it is one
 * request answered by a stream of Server-Sent Events: {@link
 * io.axoniq.framework.springcloud.query.QueryDispatchRequest} carries it out, {@link io.axoniq.framework.springcloud.query.QueryDispatchResponse} and {@link
 * io.axoniq.framework.springcloud.query.QueryDispatchFailure} carry the answer back, and {@link
 * io.axoniq.framework.springcloud.query.ServerSentEventReader} reads them without a reactive stack. {@link
 * io.axoniq.framework.springcloud.query.RemoteQueryDispatcher} sends a query, {@link
 * io.axoniq.framework.springcloud.query.SpringCloudQueryController} receives it, and {@link
 * io.axoniq.framework.springcloud.query.IncomingQueryInvoker} invokes the local handler with it.
 */
@NullMarked
package io.axoniq.framework.springcloud.query;

import org.jspecify.annotations.NullMarked;
