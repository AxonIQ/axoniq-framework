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

package org.axonframework.messaging.core;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;

import java.util.Objects;

/**
 * An {@link ApplicationContext} implementation that retrieves components from a given {@link Configuration}.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
@Internal
public class ConfigurationApplicationContext implements ApplicationContext {

    private final Configuration configuration;

    /**
     * Creates a new {@link ConfigurationApplicationContext} that retrieves components from the given {@code configuration}.
     *
     * @param configuration The configuration to retrieve components from.
     */
    public ConfigurationApplicationContext(Configuration configuration) {
        Objects.requireNonNull(configuration, "configuration may not be null");
        this.configuration = configuration;
    }

    @Override
    public <C> C component(Class<C> type, @Nullable String name) {
        return configuration.getComponent(type, name);
    }

    @Override
    public <C> C component(Class<C> type) {
        return configuration.getComponent(type);
    }
}
