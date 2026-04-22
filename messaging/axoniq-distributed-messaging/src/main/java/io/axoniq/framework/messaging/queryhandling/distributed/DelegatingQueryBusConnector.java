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

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * A {@link QueryBusConnector} implementation that wraps another {@link QueryBusConnector} and delegates all calls to
 * it.
 * <p>
 * This can be used to add additional functionality through decoration to a {@link QueryBusConnector} without having to
 * implement all methods again.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public abstract class DelegatingQueryBusConnector implements QueryBusConnector {

    protected final QueryBusConnector delegate;

    /**
     * Initialize the delegating {@link QueryBusConnector} to delegate all calls to the given {@code delegate}.
     *
     * @param delegate The {@link QueryBusConnector} to delegate all calls to.
     */
    protected DelegatingQueryBusConnector(QueryBusConnector delegate) {
        this.delegate = requireNonNull(delegate, "The delegate must not be null.");
    }

    // region [QueryBus]

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        return delegate.query(query, context);
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext context,
                                                                 int updateBufferSize) {
        return delegate.subscriptionQuery(query, context, updateBufferSize);
    }

    // endregion


    // region [Connector]

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        return delegate.subscribe(name);
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        return delegate.unsubscribe(name);
    }

    @Override
    public void onIncomingQuery(Handler handler) {
        delegate.onIncomingQuery(handler);
    }
    // endregion

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
    }
}
