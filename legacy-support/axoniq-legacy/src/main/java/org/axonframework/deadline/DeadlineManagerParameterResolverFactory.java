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

package org.axonframework.deadline;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link ParameterResolverFactory} resolving a parameter declared as {@link DeadlineManager} to a view of the
 * configured {@link AbstractDeadlineManager}, bound to the {@link ProcessingContext} the handler runs in.
 * <p>
 * It returns no resolver for a parameter of any other type, when the configuration has no {@link DeadlineManager}, or
 * when the configured one does not extend {@link AbstractDeadlineManager}. The configuration's own parameter resolver
 * then resolves the parameter to the configured component, as it did before. Registered by
 * {@link DeadlineManagerParameterResolverFactoryConfigurationEnhancer}.
 */
final class DeadlineManagerParameterResolverFactory implements ParameterResolverFactory {

    private final Configuration configuration;

    /**
     * Instantiate a factory resolving the {@link DeadlineManager} of the given {@code configuration}.
     *
     * @param configuration the configuration to look the {@link DeadlineManager} up in
     */
    DeadlineManagerParameterResolverFactory(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration may not be null.");
    }

    @Override
    public @Nullable ParameterResolver<DeadlineManager> createInstance(Executable executable,
                                                                       Parameter[] parameters,
                                                                       int parameterIndex) {
        if (parameters[parameterIndex].getType() != DeadlineManager.class) {
            return null;
        }
        return configuration.getOptionalComponent(DeadlineManager.class)
                            .filter(AbstractDeadlineManager.class::isInstance)
                            .map(AbstractDeadlineManager.class::cast)
                            .map(ContextBoundDeadlineManagerResolver::new)
                            .orElse(null);
    }

    private record ContextBoundDeadlineManagerResolver(AbstractDeadlineManager deadlineManager)
            implements ParameterResolver<DeadlineManager> {

        @Override
        public CompletableFuture<DeadlineManager> resolveParameterValue(ProcessingContext context) {
            return CompletableFuture.completedFuture(deadlineManager.forContext(context));
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return true;
        }
    }
}
