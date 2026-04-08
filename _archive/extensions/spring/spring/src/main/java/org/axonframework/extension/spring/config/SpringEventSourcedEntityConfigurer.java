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

package org.axonframework.extension.spring.config;

import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;

/**
 * A {@link ConfigurationEnhancer} implementation that will configure an Aggregate with the Axon
 * {@link Configuration}.
 *
 * @param <T>  The type of Aggregate to configure
 * @param <ID> The type of Aggregate id to configure
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @since 4.6.0
 */
@Internal
@RegistrationScope("Don't copy this enhancer in order to avoid cyclic module build in Spring Boot.")
public class SpringEventSourcedEntityConfigurer<ID, T> implements ConfigurationEnhancer {

    private final Class<T> entityType;
    private final Class<ID> idType;

    /**
     * Initializes a {@link ConfigurationEnhancer} for given {@code entityType} and {@code idType}.
     *
     * @param entityType The declared type of the entity.
     * @param idType        The type of id.
     */
    public SpringEventSourcedEntityConfigurer(Class<T> entityType, Class<ID> idType) {
        this.entityType = entityType;
        this.idType = idType;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        var eventSourcedEntityModule = EventSourcedEntityModule.autodetected(this.idType, this.entityType);
        registry.registerModule(eventSourcedEntityModule);
    }
}
