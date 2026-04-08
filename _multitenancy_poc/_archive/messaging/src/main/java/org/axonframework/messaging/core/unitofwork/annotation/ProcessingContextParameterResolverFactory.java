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

package org.axonframework.messaging.core.unitofwork.annotation;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.Priority;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

import static org.axonframework.common.Priority.LOW;

/**
 * {@link ParameterResolverFactory} implementation that provides a {@link ParameterResolver} for parameters of type
 * {@link ProcessingContext}.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Priority(LOW)
public class ProcessingContextParameterResolverFactory implements ParameterResolverFactory {

    private static final ProcessingContextParameterResolver INSTANCE = new ProcessingContextParameterResolver();

    @Nullable
    @Override
    public ParameterResolver<ProcessingContext> createInstance(Executable executable, Parameter[] parameters,
                                                               int parameterIndex) {

        Parameter parameter = parameters[parameterIndex];
        if (parameter.getType().isAssignableFrom(ProcessingContext.class)) {
            return INSTANCE;
        }
        return null;
    }

    private static class ProcessingContextParameterResolver implements ParameterResolver<ProcessingContext> {

        @Override
        public CompletableFuture<ProcessingContext> resolveParameterValue(ProcessingContext context) {
            return CompletableFuture.completedFuture(context);
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return true;
        }
    }
}
