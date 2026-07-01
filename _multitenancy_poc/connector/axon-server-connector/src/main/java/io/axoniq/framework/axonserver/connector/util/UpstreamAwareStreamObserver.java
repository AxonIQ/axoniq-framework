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

package io.axoniq.framework.axonserver.connector.util;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

/**
 * Convenience implementation of a StreamObserver that provides access to the request stream, which allows cancellation
 * of the call, flow control, etc.
 *
 * @param <ResT> the type of response sent by the server
 * @author Allard Buijze
 * @see ClientCallStreamObserver
 * @since 4.2
 */
public abstract class UpstreamAwareStreamObserver<ResT> implements ClientResponseObserver<Object, ResT> {

    private ClientCallStreamObserver<?> requestStream;

    @Override
    public void beforeStart(ClientCallStreamObserver<Object> requestStream) {
        this.requestStream = requestStream;
    }

    /**
     * Returns the request stream observer which allows interaction with the client stream.
     *
     * @return the request stream observer which allows interaction with the client stream
     */
    public ClientCallStreamObserver<?> getRequestStream() {
        return requestStream;
    }

    /**
     * Completes the request steam related to this stream observer. Ignores exceptions that may occur (for instance if
     * the request stream is already half-closed).
     */
    protected void completeRequestStream() {
        if (requestStream != null) {
            try {
                requestStream.onCompleted();
            } catch (Exception ex) {
                // Ignore exceptions on completing the request stream, may already have been closed
            }
        }
    }
}
