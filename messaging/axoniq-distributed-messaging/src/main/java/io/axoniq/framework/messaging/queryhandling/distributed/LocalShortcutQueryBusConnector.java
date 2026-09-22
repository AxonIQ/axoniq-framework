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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link QueryBusConnector} that offers a "local shortcut".
 * <p>
 * This shortcut triggers for
 * {@link org.axonframework.messaging.queryhandling.QueryBus#query(QueryMessage, ProcessingContext) point-to-point
 * queries} matching a {@link LocalQueryDispatchPredicate}, and for which the local segment subscribed a handler, the
 * query is handed to the local segment directly instead of being {@link #query(QueryMessage, ProcessingContext) routed}
 * through the wrapped connector. Furthermore, the shortcut only activates when the local segment can actually handle
 * the command, tracked through the {@link #subscribe(QualifiedName)}/{@link #unsubscribe(QualifiedName)} calls this
 * connector observes.
 * <p>
 * Short-cut queries are handled through the same {@link QueryBusConnector.Handler} as any other
 * {@code QueryBusConnector}. As such, they are queued onto and executed by the same bounded, priority-ordered worker
 * pool as commands arriving from remote segments. A burst of locally-preferred commands is therefore subject to the
 * same back-pressure and cannot flood the local segment beyond what it would already accept from remote traffic.
 * <p>
 * The shortcut applies to point-to-point {@link #query(QueryMessage, ProcessingContext) queries} <b>only</b>.
 * {@link #subscriptionQuery(QueryMessage, ProcessingContext, int) Subscription queries} are always routed through the
 * wrapped connector, as their update registrations must be coordinated across all segments.
 *
 * @author Allard Buijze
 * @see LocalQueryDispatchPredicate
 * @see LocalShortcutQueryBusConnectorConfigurationEnhancer
 * @since 5.4.0
 */
public class LocalShortcutQueryBusConnector extends DelegatingQueryBusConnector {

    private final LocalQueryDispatchPredicate shortcutPredicate;
    private final Set<QualifiedName> subscriptions = ConcurrentHashMap.newKeySet();

    private volatile @Nullable Handler localHandler;

    /**
     * Initialize a connector with the given {@code delegate}, using the given {@code shortcutPredicate} to decide
     * whether to shortcut a query yes or no.
     *
     * @param delegate          the {@link QueryBusConnector} to delegate to when not querying locally
     * @param shortcutPredicate the predicate deciding whether a query should be dispatched to the local segment
     *                          directly
     */
    public LocalShortcutQueryBusConnector(QueryBusConnector delegate,
                                          LocalQueryDispatchPredicate shortcutPredicate) {
        super(delegate);
        this.shortcutPredicate = Objects.requireNonNull(
                shortcutPredicate, "The LocalQueryDispatchPredicate must not be null."
        );
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        Handler handler = this.localHandler;

        if (handler != null
                && subscriptions.contains(query.type().qualifiedName())
                && shortcutPredicate.shouldDispatchLocally(query, context)) {
            return handler.query(query);
        }

        return super.query(query, context);
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        subscriptions.add(name);
        return super.subscribe(name);
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        subscriptions.remove(name);
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
        descriptor.describeProperty("shortcutPredicate", shortcutPredicate);
        descriptor.describeProperty("subscriptions", subscriptions);
    }
}
