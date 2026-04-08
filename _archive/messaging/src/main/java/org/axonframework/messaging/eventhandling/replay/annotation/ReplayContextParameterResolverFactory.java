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

package org.axonframework.messaging.eventhandling.replay.annotation;

import org.jspecify.annotations.Nullable;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of the {@link ParameterResolverFactory} which resolves the parameter annotated with
 * {@link ReplayContext}.
 * <p>
 * Will resolve the parameter from the {@link ProcessingContext} if it contains a {@link ReplayToken} with a matching
 * context of that type. Otherwise, it will resolve always to null. This parameter resolver will always match to prevent
 * missing event handlers.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class ReplayContextParameterResolverFactory implements ParameterResolverFactory {

    @Nullable
    @Override
    public ParameterResolver<Object> createInstance(Executable executable,
                                                    Parameter[] parameters,
                                                    int parameterIndex) {
        Parameter parameter = parameters[parameterIndex];
        if (parameter.isAnnotationPresent(ReplayContext.class)) {
            return new ReplayContextParameterResolver(parameter.getType());
        }
        return null;
    }

    private static class ReplayContextParameterResolver implements ParameterResolver<Object> {

        private final Class<?> type;

        public ReplayContextParameterResolver(Class<?> type) {
            this.type = type;
        }

        @Override
        public CompletableFuture<Object> resolveParameterValue(ProcessingContext context) {
            TrackingToken token = TrackingToken.fromContext(context).orElse(null);
            if (token == null) {
                return CompletableFuture.completedFuture(null);
            }

            return CompletableFuture.completedFuture(
                    ReplayToken.replayContext(token, type, context.component(Converter.class)).orElse(null)
            );
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return Message.fromContext(context) instanceof EventMessage;
        }
    }
}
