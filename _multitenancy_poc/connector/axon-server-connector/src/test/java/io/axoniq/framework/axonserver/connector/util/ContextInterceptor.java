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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * A gRPC {@link ServerInterceptor} setting the context to the {@link Context}. Uses the key {@code "AxonIQ-Context"} to
 * attach the current context.
 *
 * @author Marc Gathier
 */
public class ContextInterceptor implements ServerInterceptor {

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall,
                                                       Metadata metadata,
                                                       ServerCallHandler<T, R> serverCallHandler) {
        String context = metadata.get(PlatformService.AXON_IQ_CONTEXT);
        if (context == null) {
            context = "default";
        }
        Context updatedGrpcContext = Context.current()
                                            .withValue(PlatformService.CONTEXT_KEY, context);
        return Contexts.interceptCall(updatedGrpcContext, serverCall, metadata, serverCallHandler);
    }
}
