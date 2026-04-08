/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventhandling.processing.streaming.token.annotation;


import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of a {@link ParameterResolverFactory} that resolves the {@link TrackingToken} from the
 * {@link ProcessingContext} whenever it's available.
 *
 * @author Allard Buijze
 * @since 3.0.0
 */
public class TrackingTokenParameterResolverFactory implements ParameterResolverFactory {

    private static final TrackingTokenParameterResolver RESOLVER = new TrackingTokenParameterResolver();

    @Nullable
    @Override
    public ParameterResolver<TrackingToken> createInstance(Executable executable,
                                                           Parameter[] parameters,
                                                           int parameterIndex) {
        if (TrackingToken.class.equals(parameters[parameterIndex].getType())) {
            return RESOLVER;
        }
        return null;
    }

    private static class TrackingTokenParameterResolver implements ParameterResolver<TrackingToken> {

        @Override
        public CompletableFuture<TrackingToken> resolveParameterValue(ProcessingContext context) {
            return CompletableFuture.completedFuture(
                    TrackingToken.fromContext(context)
                                 .map(this::unwrap)
                                 .orElse(null)
            );
        }

        private TrackingToken unwrap(TrackingToken trackingToken) {
            return WrappedToken.unwrapLowerBound(trackingToken);
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return context.containsResource(TrackingToken.RESOURCE_KEY);
        }
    }
}
