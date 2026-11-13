/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.modelling.annotation.EntityIdResolverDefinition;

/**
 * {@link ConfigurationEnhancer} registering {@link AggregateDeadlineEntityIdResolverDefinition} as the application-wide
 * {@link EntityIdResolverDefinition} default.
 * <p>
 * This registration only benefits an {@code @EventSourcedEntity}/{@code @EventSourced} entity: see
 * {@link AggregateDeadlineEntityIdResolverDefinition}'s class Javadoc for why a migrated state-stored aggregate or a
 * declaratively configured entity module does not pick up this default.
 *
 * @author Steven van Beelen
 * @see AggregateDeadlineEntityIdResolverDefinition
 * @since 5.4.0
 */
public class AggregateDeadlineEntityIdResolverConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(
                EntityIdResolverDefinition.class,
                c -> new AggregateDeadlineEntityIdResolverDefinition()
        );
    }
}
