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
