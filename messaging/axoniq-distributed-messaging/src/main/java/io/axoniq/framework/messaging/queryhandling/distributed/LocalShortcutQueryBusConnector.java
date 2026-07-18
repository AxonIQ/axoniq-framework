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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link QueryBusConnector} that offers a "local shortcut": for point-to-point queries matching a
 * {@link LocalQueryDispatchPredicate}, and for which the local segment subscribed a handler, the query is handed to the
 * local segment directly instead of being {@link #query(QueryMessage, ProcessingContext) routed} through the wrapped
 * connector.
 * <p>
 * The shortcut reuses the very same local-handling path that the {@link DistributedQueryBus} exposes to incoming,
 * remotely-routed queries: the {@link Handler} registered through {@link #onIncomingQuery(Handler)}. A short-cut query
 * therefore behaves exactly as if it had been routed to this segment - same handler invocation, same response
 * handling - only without leaving the JVM. Because this connector wraps the payload-converting connector (rather than
 * the other way around), the shortcut also avoids the payload serialization round-trip.
 * <p>
 * Because short-cut queries are handled through this same {@link Handler}, they are queued onto and executed by the
 * same bounded, priority-ordered worker pool as queries arriving from remote segments. A burst of locally-preferred
 * queries is therefore subject to the same back-pressure and cannot flood the local segment beyond what it would
 * already accept from remote traffic.
 * <p>
 * The shortcut only kicks in when the local segment can actually handle the query, tracked through the
 * {@link #subscribe(QualifiedName)}/{@link #unsubscribe(QualifiedName)} calls this connector observes. When the query's
 * {@link QueryMessage#type() type} is not locally subscribed, the query is routed through the wrapped connector as
 * usual, regardless of the predicate. This prevents a locally-preferred query that this node does not handle from
 * failing instead of being routed to a segment that does.
 * <p>
 * The shortcut applies to point-to-point {@link #query(QueryMessage, ProcessingContext) queries} only.
 * {@link #subscriptionQuery(QueryMessage, ProcessingContext, int) Subscription queries} are always routed through the
 * wrapped connector, as their update registrations must be coordinated across all segments.
 *
 * @author Allard Buijze
 * @see LocalQueryDispatchPredicate
 * @see LocalShortcutQueryBusConnectorConfigurationEnhancer
 * @since 5.3.0
 */
public class LocalShortcutQueryBusConnector extends DelegatingQueryBusConnector {

    private final LocalQueryDispatchPredicate localDispatchPredicate;
    private final Set<QualifiedName> localSubscriptions = ConcurrentHashMap.newKeySet();

    private volatile @Nullable Handler localHandler;

    /**
     * Initialize the connector to delegate to the given {@code delegate}, taking a local shortcut for queries accepted
     * by the given {@code localDispatchPredicate} that are also handled by the local segment.
     *
     * @param delegate               the {@link QueryBusConnector} to delegate to when not querying locally
     * @param localDispatchPredicate the predicate deciding whether a query should be dispatched to the local segment
     *                               directly
     */
    public LocalShortcutQueryBusConnector(QueryBusConnector delegate,
                                          LocalQueryDispatchPredicate localDispatchPredicate) {
        super(delegate);
        this.localDispatchPredicate =
                Objects.requireNonNull(localDispatchPredicate, "The localDispatchPredicate must not be null.");
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        Handler handler = this.localHandler;
        if (handler != null
                && localSubscriptions.contains(query.type().qualifiedName())
                && localDispatchPredicate.shouldDispatchLocally(query, context)) {
            return handler.query(query);
        }
        return super.query(query, context);
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        localSubscriptions.add(name);
        return super.subscribe(name);
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        localSubscriptions.remove(name);
        return super.unsubscribe(name);
    }

    @Override
    public void onIncomingQuery(Handler handler) {
        this.localHandler = handler;
        super.onIncomingQuery(handler);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("localDispatchPredicate", localDispatchPredicate);
        descriptor.describeProperty("localSubscriptions", localSubscriptions);
    }
}
