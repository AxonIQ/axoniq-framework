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

package org.axonframework.messaging.core.annotation;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.Priority;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;

/**
 * A {@link ParameterResolverFactory} implementation for simple resource injections. Uses the
 * {@link FixedValueParameterResolver} to inject a resource as a fixed value on message handling if the resource equals
 * a message handling method parameter.
 */
@Priority(Priority.LOWER)
public class SimpleResourceParameterResolverFactory implements ParameterResolverFactory {

    private final Iterable<?> resources;

    /**
     * Initialize the ParameterResolverFactory to inject the given {@code resource} in applicable parameters.
     *
     * @param resources The resource to inject
     */
    public SimpleResourceParameterResolverFactory(Iterable<?> resources) {
        this.resources = resources;
    }

    @Nullable
    @Override
    public ParameterResolver<?> createInstance(Executable executable,
                                               Parameter[] parameters,
                                               int parameterIndex) {
        for (Object resource : resources) {
            if (parameters[parameterIndex].getType().isInstance(resource)) {
                return new FixedValueParameterResolver<>(resource);
            }
        }
        return null;
    }
}
