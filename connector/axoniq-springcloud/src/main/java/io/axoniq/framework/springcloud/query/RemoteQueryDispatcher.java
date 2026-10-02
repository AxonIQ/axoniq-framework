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

import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

/**
 * Sends a query to another member of the cluster and reports the responses it streams back.
 * <p>
 * Extracted as an interface so that routing a query can be exercised without a member to send it to.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public interface RemoteQueryDispatcher {

    /**
     * Sends the given {@code query} to the given {@code member}, reporting the responses it streams back.
     * <p>
     * The returned stream is live: responses appear on it as the answering member produces them, and it completes
     * when that member has answered in full. A failure reported by the answering member, or one reaching it, ends the
     * stream in error. Closing the stream stops the member from being read any further.
     *
     * @param member the member to send the query to
     * @param query  the query to send
     * @return the stream of responses the given {@code member} answers with
     */
    MessageStream<QueryResponseMessage> dispatch(Member member, QueryMessage query);

    /**
     * Opens the stream of updates the given {@code query} produces on the given {@code member}.
     * <p>
     * A streaming query like any other, except that it does not end of its own accord: the member keeps it open,
     * emitting an update whenever the state the query reads changes, until it is released or the member reports a
     * failure. Closing the stream releases the subscription.
     * <p>
     * Carries updates alone. The initial result of a subscription query is a query like any other, asked for with
     * {@link #dispatch(Member, QueryMessage)} once this member's update stream is open, so that an update emitted
     * while that result is being produced still has somewhere to arrive. {@code onOpen} is what says when that is:
     * it runs once the member has registered the subscription, and so before any update it emits.
     *
     * @param member           the member to open the update stream on
     * @param query            the query to subscribe with
     * @param updateBufferSize how many updates to hold for this subscriber before failing the subscription
     * @param listener         told when the member has registered the subscription, and when it reports the
     *                         subscription over
     * @return the updates the given {@code member} emits for the given {@code query}
     */
    MessageStream<QueryResponseMessage> openSubscriptionQueryUpdateStream(Member member,
                                                                          QueryMessage query,
                                                                          int updateBufferSize,
                                                                          SubscriptionListener listener);

    /**
     * What a member reports about a subscription besides the updates themselves.
     * <p>
     * Both are things the stream of updates cannot say for itself. It carries updates, and it ends; neither tells the
     * subscriber that the member has registered the subscription, nor whether the end means the subscription is over
     * or merely that this member stopped answering.
     */
    interface SubscriptionListener {

        /**
         * The member has registered the subscription, and so will not miss an update from here on.
         * <p>
         * Not called at all when the stream fails before reaching that point, which the stream itself reports.
         */
        void opened();

        /**
         * The member reported the subscription over: there will never be another update to it.
         * <p>
         * Distinct from the stream ending, which a member leaving the cluster also does. That ends this member's part
         * in the subscription; this ends the subscription.
         */
        void completed();
    }
}
