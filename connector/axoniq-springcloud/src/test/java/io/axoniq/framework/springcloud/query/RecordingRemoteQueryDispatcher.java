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

package io.axoniq.framework.springcloud.query;

import io.axoniq.framework.springcloud.query.RemoteQueryDispatcher.SubscriptionListener;
import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link RemoteQueryDispatcher} recording what it was asked to send, answering with what it was told to.
 *
 * @author Allard Buijze
 */
class RecordingRemoteQueryDispatcher implements RemoteQueryDispatcher {

    private final List<Dispatch> dispatches = new CopyOnWriteArrayList<>();
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    private volatile List<QueryResponseMessage> responses = List.of();
    private volatile Throwable cause;
    private volatile MessageStream<QueryResponseMessage> stream;
    private volatile boolean opensImmediately = true;
    private volatile Runnable whileOpening = () -> {
    };

    public RecordingRemoteQueryDispatcher answeringWith(QueryResponseMessage... responses) {
        this.responses = List.of(responses);
        this.cause = null;
        return this;
    }

    /**
     * Answers with the given {@code stream}, so that a query can be left open for as long as a test needs it to be.
     */
    public RecordingRemoteQueryDispatcher answeringWith(MessageStream<QueryResponseMessage> stream) {
        this.stream = stream;
        this.cause = null;
        return this;
    }

    public RecordingRemoteQueryDispatcher failingWith(Throwable cause) {
        this.cause = cause;
        return this;
    }

    public List<Dispatch> dispatches() {
        return List.copyOf(dispatches);
    }

    public List<Member> members() {
        return dispatches.stream().map(Dispatch::member).toList();
    }

    @Override
    public MessageStream<QueryResponseMessage> dispatch(Member member, QueryMessage query) {
        dispatches.add(new Dispatch(member, query));
        Throwable failure = cause;
        if (failure != null) {
            return MessageStream.failed(failure);
        }
        MessageStream<QueryResponseMessage> answer = stream;
        return answer == null ? MessageStream.fromIterable(responses) : answer;
    }

    @Override
    public MessageStream<QueryResponseMessage> openSubscriptionQueryUpdateStream(Member member,
                                                                                 QueryMessage query,
                                                                                 int updateBufferSize,
                                                                                 SubscriptionListener listener) {
        Subscription subscription = new Subscription(member, query, updateBufferSize, listener);
        subscriptions.add(subscription);
        whileOpening.run();
        Throwable failure = cause;
        if (failure != null) {
            return MessageStream.failed(failure);
        }
        if (opensImmediately) {
            subscription.open();
        }
        return subscription.updates;
    }

    /**
     * Leaves every subscription unopened until {@link #open(Member)} says so, as a member that has not answered yet
     * leaves it.
     */
    public RecordingRemoteQueryDispatcher openingOnDemand() {
        this.opensImmediately = false;
        return this;
    }

    /**
     * Runs the given {@code action} each time a subscription is opened, so that a test can change the cluster while a
     * subscription is still reaching the members it was told to reach.
     */
    public RecordingRemoteQueryDispatcher whileOpening(Runnable action) {
        this.whileOpening = action;
        return this;
    }

    /**
     * Reports the subscription opened for the given {@code member} as registered on that member.
     */
    public void open(Member member) {
        subscriptionOn(member).open();
    }

    /**
     * Returns the subscriptions opened, in the order they were opened.
     */
    public List<Subscription> subscriptions() {
        return List.copyOf(subscriptions);
    }

    /**
     * Emits the given {@code update} on the subscription opened for the given {@code member}.
     */
    public void emit(Member member, QueryResponseMessage update) {
        subscriptionOn(member).updates.offer(update, Context.empty());
    }

    /**
     * Ends the subscription opened for the given {@code member} with the given {@code failure}.
     */
    public void fail(Member member, Throwable failure) {
        subscriptionOn(member).updates.sealExceptionally(failure);
    }

    /**
     * Ends the stream opened for the given {@code member} without a failure, as a member leaving the cluster does.
     * Says nothing about the subscription itself.
     */
    public void stopAnswering(Member member) {
        subscriptionOn(member).updates.seal();
    }

    /**
     * Reports, on behalf of the given {@code member}, that the subscription is over: there will never be another
     * update to it.
     */
    public void completeSubscription(Member member) {
        Subscription subscription = subscriptionOn(member);
        subscription.listener.completed();
        subscription.updates.seal();
    }

    private Subscription subscriptionOn(Member member) {
        return subscriptions.stream()
                            .filter(subscription -> subscription.member().equals(member))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException(
                                    "No subscription was opened on [" + member.name() + "]."
                            ));
    }

    public record Dispatch(Member member, QueryMessage query) {

    }

    /**
     * One subscription this dispatcher was asked to open, and the stream of updates answering it.
     */
    public static final class Subscription {

        private final Member member;
        private final QueryMessage query;
        private final int updateBufferSize;
        private final SubscriptionListener listener;
        private final QueueMessageStream<QueryResponseMessage> updates = new QueueMessageStream<>();

        private Subscription(Member member,
                             QueryMessage query,
                             int updateBufferSize,
                             SubscriptionListener listener) {
            this.member = member;
            this.query = query;
            this.updateBufferSize = updateBufferSize;
            this.listener = listener;
        }

        private void open() {
            listener.opened();
        }

        public Member member() {
            return member;
        }

        public QueryMessage query() {
            return query;
        }

        public int updateBufferSize() {
            return updateBufferSize;
        }

        public boolean released() {
            return updates.isCompleted();
        }
    }
}
