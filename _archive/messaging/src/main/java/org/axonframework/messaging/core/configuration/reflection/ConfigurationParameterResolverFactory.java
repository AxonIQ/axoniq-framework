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

package org.axonframework.messaging.core.configuration.reflection;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.Priority;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.annotation.FixedValueParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.Objects;

import static org.axonframework.common.Priority.LOW;

/**
 * A {@code ParameterResolverFactory} implementation that resolves parameters from available components in the
 * {@link Configuration} instance it was configured with.
 * <p>
 * This implementation is usually autoconfigured when using the Configuration API.
 *
 * @author Allard Buijze
 * @since 3.0.2
 */
@Priority(LOW)
public class ConfigurationParameterResolverFactory implements ParameterResolverFactory {

    private final Configuration configuration;

    /**
     * Initialize an instance using given {@code configuration} to supply the value to resolve parameters with.
     *
     * @param configuration The configuration to look for component with.
     */
    public ConfigurationParameterResolverFactory(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration cannot be null.");
    }

    @Nullable
    @Override
    public ParameterResolver<?> createInstance(Executable executable,
                                               Parameter[] parameters,
                                               int parameterIndex) {
        Class<?> componentType = parameters[parameterIndex].getType();
        return configuration.getOptionalComponent(componentType)
                            .map(FixedValueParameterResolver::new)
                            .orElse(null);
    }
}
