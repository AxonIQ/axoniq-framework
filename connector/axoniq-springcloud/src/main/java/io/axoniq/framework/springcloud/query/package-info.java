/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * The query side of the Spring Cloud connector: the connector distributing queries and subscription queries, and the
 * HTTP transport carrying them between members of the cluster.
 * <p>
 * {@link SpringCloudQueryBusConnector} rotates each query over the members advertising its name, and either invokes
 * this application's own handler when the chosen member is the local one, or hands the query to the transport. A query
 * may be answered any number of times, so it is one request answered by a stream of Server-Sent Events:
 * {@link QueryDispatchRequest} carries it out, {@link QueryDispatchResponse} and {@link QueryDispatchFailure} carry the
 * answer back, and {@link ServerSentEventReader} reads them without a reactive stack. {@link RemoteQueryDispatcher}
 * sends a query, {@link SpringCloudQueryController} receives it, and {@link IncomingQueryInvoker} invokes the local
 * handler with it, writing every response to a {@link QueryResponseSink}, which {@link SseQueryResponseSink} adapts to
 * the response stream.
 * <p>
 * A subscription query reaches every member advertising its name: each one receives a
 * {@link SubscriptionQueryRequest}, registers the subscription, and streams the updates it emits back over the same
 * kind of response stream. The initial result is asked for as a regular query.
 */
@NullMarked
package io.axoniq.framework.springcloud.query;

import org.jspecify.annotations.NullMarked;
