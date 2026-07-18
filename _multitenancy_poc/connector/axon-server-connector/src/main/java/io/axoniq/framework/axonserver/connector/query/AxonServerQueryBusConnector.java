/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the
 * specific language governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.axonserver.connector.query;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AxonServerQueryBusConnector is an implementation of {@link io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector}
 * that connects to AxonServer to enable the dispatching and receiving of queries.
 *
 * @author Steven van Beelen, Allard Buijze, Jan Galinski
 * @since 5.0.0
 */
public class AxonServerQueryBusConnector extends AbstractAxonServerQueryBusConnector {

    private final AxonServerConnection connection;
    private final Map<QualifiedName, Registration> subscriptions = new ConcurrentHashMap<>();
    private final LocalSegmentAdapter localSegmentAdapter = newLocalSegmentAdapter();

    public AxonServerQueryBusConnector(AxonServerConnection connection,
                                       AxonServerConfiguration configuration) {
        this(connection, configuration, null);
    }

    public AxonServerQueryBusConnector(AxonServerConnection connection,
                                       AxonServerConfiguration configuration,
                                       @Nullable MessageConverter converter) {
        super(configuration.getClientId(), configuration.getComponentName(), converter);
        this.connection = connection;
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        return doSubscribe(connection, name, localSegmentAdapter, subscriptions);
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        return doUnsubscribe(name, subscriptions);
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query,
                                                     @Nullable ProcessingContext context) {
        return doQuery(query, connection);
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                  @Nullable ProcessingContext context,
                                                                  int updateBufferSize) {
        return doSubscriptionQuery(query, connection, updateBufferSize);
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return doDisconnect(connection, localSegmentAdapter);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        super.describeTo(descriptor);
        descriptor.describeProperty("connection", connection);
    }
}
