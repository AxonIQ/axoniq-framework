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

package org.axonframework.eventsourcing.annotation;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.CriteriaResolver;

/**
 * Defines how an {@link AnnotationBasedEventCriteriaResolver} should be constructed for an {@link EventSourcedEntity}
 * annotated class. This is the default implementation of the {@link CriteriaResolverDefinition} for the
 * {@link EventSourcedEntity} annotation.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class AnnotationBasedEventCriteriaResolverDefinition implements CriteriaResolverDefinition {

    @Override
    public <E, ID> CriteriaResolver<ID> createEventCriteriaResolver(
            Class<E> entityType,
            Class<ID> idType,
            Configuration configuration
    ) {
        return new AnnotationBasedEventCriteriaResolver<>(entityType, idType, configuration);
    }
}
