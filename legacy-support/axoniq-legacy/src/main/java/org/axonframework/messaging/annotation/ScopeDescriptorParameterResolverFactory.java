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

package org.axonframework.messaging.annotation;

import org.axonframework.messaging.NoScopeDescriptor;
import org.axonframework.messaging.Scope;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

/**
 * Factory for a {@link ScopeDescriptor} {@link ParameterResolver}. Will return the result of
 * {@link Scope#describeCurrentScope()}. If no current scope is active, {@link NoScopeDescriptor#INSTANCE} will be
 * returned.
 *
 * @author Steven van Beelen
 * @since 4.5
 */
public class ScopeDescriptorParameterResolverFactory implements ParameterResolverFactory {

    @Nullable
    @Override
    public ParameterResolver<ScopeDescriptor> createInstance(Executable executable,
                                                             Parameter[] parameters,
                                                             int parameterIndex) {
        return ScopeDescriptor.class.isAssignableFrom(parameters[parameterIndex].getType())
                ? new ScopeDescriptorParameterResolver() : null;
    }

    private static class ScopeDescriptorParameterResolver implements ParameterResolver<ScopeDescriptor> {

        @Override
        public CompletableFuture<ScopeDescriptor> resolveParameterValue(ProcessingContext context) {
            try {
                return CompletableFuture.completedFuture(Scope.describeCurrentScope());
            } catch (IllegalStateException e) {
                return CompletableFuture.completedFuture(NoScopeDescriptor.INSTANCE);
            }
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return true;
        }
    }
}
