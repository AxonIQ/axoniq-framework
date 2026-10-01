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

/**
 * Where a member writes the responses to a query it is answering for another member.
 * <p>
 * This is what keeps answering a query testable without a web stack: the endpoint adapts a response stream to this,
 * and everything deciding what to write works against it rather than against the stream itself.
 * <p>
 * A sink is written to until it is terminated, which either {@link #error(QueryDispatchFailure)} or {@link #complete()}
 * does. Nothing is written after that.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public interface QueryResponseSink {

    /**
     * Registers a {@code listener} to run once this sink can no longer be written to, for a reason other than the
     * query having been answered — the member that asked stopped reading, or waited longer than it was given.
     * <p>
     * Only the sink knows how its transport reports that, which is why the listener is registered here rather than
     * discovered by whatever writes to it. Answering a query that nothing is reading is wasted work, and the handler's
     * response stream would otherwise be held until it happens to produce a response that fails to write — which a
     * handler that has gone quiet never does.
     *
     * @param listener runs when this sink can no longer be written to
     */
    void onUnavailable(Runnable listener);

    /**
     * Writes one response to the query being answered.
     *
     * @param response the response to write
     */
    void response(QueryDispatchResponse response);

    /**
     * Ends the response stream, reporting that the query failed.
     *
     * @param error the failure that ended the response stream
     */
    void error(QueryDispatchFailure error);

    /**
     * Ends the response stream, reporting that the query was answered in full.
     */
    void complete();
}
