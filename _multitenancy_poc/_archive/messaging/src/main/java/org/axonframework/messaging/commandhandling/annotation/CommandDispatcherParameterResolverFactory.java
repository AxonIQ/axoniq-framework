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

package org.axonframework.messaging.commandhandling.annotation;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.commandhandling.gateway.CommandDispatcher;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

/**
 * {@link ParameterResolverFactory} that ensures the {@link CommandDispatcher} is resolved in the context of the current
 * {@link ProcessingContext}.
 * <p>
 * For any message handler that declares this parameter, it will call
 * {@link CommandDispatcher#forContext(ProcessingContext)} to create the appender.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public class CommandDispatcherParameterResolverFactory implements ParameterResolverFactory {

    @Nullable
    @Override
    public ParameterResolver<CommandDispatcher> createInstance(Executable executable,
                                                               Parameter[] parameters,
                                                               int parameterIndex) {
        if (!CommandDispatcher.class.isAssignableFrom(parameters[parameterIndex].getType())) {
            return null;
        }

        return new ParameterResolver<>() {
            @Override
            public CompletableFuture<CommandDispatcher> resolveParameterValue(ProcessingContext context) {
                return CompletableFuture.completedFuture(CommandDispatcher.forContext(context));
            }

            @Override
            public boolean matches(ProcessingContext context) {
                return true;
            }
        };
    }
}
