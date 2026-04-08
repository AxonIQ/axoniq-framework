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

package org.axonframework.config;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.modelling.saga.AbstractResourceInjector;

import java.util.Optional;

/**
 * ResourceInjector implementation that injects resources defined in the Axon Configuration.
 */
public class ConfigurationResourceInjector extends AbstractResourceInjector {

    private final Configuration configuration;

    /**
     * Initializes the ResourceInjector to inject the resources found in the given {@code configuration}.
     *
     * @param configuration the Configuration to find injectable resources in
     */
    public ConfigurationResourceInjector(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected <R> Optional<R> findResource(Class<R> requiredType) {
        return configuration.getOptionalComponent(requiredType);
    }
}
