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

package io.axoniq.framework.springcloud.transport;

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
}
