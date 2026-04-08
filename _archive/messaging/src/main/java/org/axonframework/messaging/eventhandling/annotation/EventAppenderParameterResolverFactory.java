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

package org.axonframework.messaging.eventhandling.annotation;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

/**
 * {@link ParameterResolverFactory} that ensures the {@link EventAppender} is resolved in the context of the current
 * {@link ProcessingContext}.
 * <p>
 * For any message handler that declares this parameter, it will call
 * {@link EventAppender#forContext(ProcessingContext)} to create the appender.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class EventAppenderParameterResolverFactory implements ParameterResolverFactory {

    @Nullable
    @Override
    public ParameterResolver<EventAppender> createInstance(Executable executable,
                                                           Parameter[] parameters,
                                                           int parameterIndex) {
        if (EventAppender.class.isAssignableFrom(parameters[parameterIndex].getType())) {
            return new ParameterResolver<>() {
                @Override
                public CompletableFuture<EventAppender> resolveParameterValue(ProcessingContext context) {
                    return CompletableFuture.completedFuture(EventAppender.forContext(context));
                }

                @Override
                public boolean matches(ProcessingContext context) {
                    return true;
                }
            };
        }
        return null;
    }
}
