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

package io.axoniq.framework.springcloud.util;

import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.Registration;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link QueryBusConnector.Handler} recording the queries handed to it, answering with what it was told to.
 *
 * @author Allard Buijze
 */
public class RecordingQueryHandler implements QueryBusConnector.Handler {

    private final List<QueryMessage> queries = new CopyOnWriteArrayList<>();

    private volatile List<QueryResponseMessage> responses = List.of();
    private volatile Throwable cause;

    public RecordingQueryHandler answeringWith(QueryResponseMessage... responses) {
        this.responses = List.of(responses);
        this.cause = null;
        return this;
    }

    public RecordingQueryHandler failingWith(Throwable cause) {
        this.cause = cause;
        return this;
    }

    public List<QueryMessage> queries() {
        return List.copyOf(queries);
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query) {
        queries.add(query);
        Throwable failure = cause;
        return failure == null
                ? MessageStream.fromIterable(responses)
                : MessageStream.failed(failure);
    }

    @Override
    public Registration registerUpdateHandler(QueryMessage subscriptionQueryMessage,
                                              QueryBusConnector.UpdateCallback updateCallback) {
        throw new UnsupportedOperationException("Subscription queries are not exercised by this handler.");
    }
}
