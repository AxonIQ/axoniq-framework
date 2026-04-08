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

package org.axonframework.modelling.annotation;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.modelling.entity.annotation.AnnotatedEntityMetamodel;
import org.axonframework.modelling.EntityIdResolver;

/**
 * Definition for an {@link EntityIdResolver} that uses annotation to resolve the entity identifier.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class AnnotationBasedEntityIdResolverDefinition implements EntityIdResolverDefinition {

    @Override
    public <E, ID> EntityIdResolver<ID> createIdResolver(Class<E> entityType,
                                                         Class<ID> idType,
                                                         AnnotatedEntityMetamodel<E> entityMetamodel,
                                                         Configuration configuration
    ) {
        return new AnnotationBasedEntityIdResolver<>();
    }
}
