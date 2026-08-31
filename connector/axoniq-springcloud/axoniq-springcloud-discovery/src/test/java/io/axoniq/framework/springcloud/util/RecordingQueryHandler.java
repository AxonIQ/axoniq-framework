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
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A {@link QueryBusConnector.Handler} recording the queries handed to it, answering with what it was told to.
 *
 * @author Allard Buijze
 */
public class RecordingQueryHandler implements QueryBusConnector.Handler {

    private final List<QueryMessage> queries = new CopyOnWriteArrayList<>();
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final List<Subscription> cancelled = new CopyOnWriteArrayList<>();

    private volatile List<QueryResponseMessage> responses = List.of();
    private volatile Throwable cause;
    private volatile Consumer<QueryBusConnector.UpdateCallback> whileRegistering = callback -> {
    };

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
        Subscription subscription = new Subscription(subscriptionQueryMessage, updateCallback);
        subscriptions.add(subscription);
        whileRegistering.accept(updateCallback);
        return () -> {
            subscription.cancelled = true;
            cancelled.add(subscription);
            return subscriptions.remove(subscription);
        };
    }

    /**
     * Returns the subscriptions registered on this handler and not yet cancelled.
     */
    public List<Subscription> subscriptions() {
        return List.copyOf(subscriptions);
    }

    /**
     * Returns the subscriptions that were cancelled, in the order they were registered.
     */
    public List<Subscription> cancelledSubscriptions() {
        return cancelled.stream().filter(Subscription::cancelled).toList();
    }

    /**
     * Runs the given {@code action} while an update handler is being registered, as a handler emitting the state it
     * is asked to watch does, before whatever registers it has wired up a reader.
     */
    public RecordingQueryHandler whileRegistering(Consumer<QueryBusConnector.UpdateCallback> action) {
        this.whileRegistering = action;
        return this;
    }

    /**
     * Emits the given {@code update} on every subscription registered on this handler.
     */
    public void emit(SubscriptionQueryUpdateMessage update) {
        subscriptions.forEach(subscription -> subscription.callback().sendUpdate(update));
    }

    /**
     * One subscription registered on this handler.
     */
    public static final class Subscription {

        private final QueryMessage query;
        private final QueryBusConnector.UpdateCallback callback;

        private volatile boolean cancelled;

        private Subscription(QueryMessage query, QueryBusConnector.UpdateCallback callback) {
            this.query = query;
            this.callback = callback;
        }

        public QueryMessage query() {
            return query;
        }

        public QueryBusConnector.UpdateCallback callback() {
            return callback;
        }

        public boolean cancelled() {
            return cancelled;
        }
    }
}
