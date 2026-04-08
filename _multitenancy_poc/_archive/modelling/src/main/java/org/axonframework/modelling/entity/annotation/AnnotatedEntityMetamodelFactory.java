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

package org.axonframework.modelling.entity.annotation;

import org.axonframework.common.annotation.Internal;

/**
 * Factory for creating {@link AnnotatedEntityMetamodel} instances for a given entity type. Used by the
 * {@link AnnotatedEntityMetamodel} to create child metamodels using the same configuration that were used to
 * create the parent metamodel.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@FunctionalInterface
@Internal
public interface AnnotatedEntityMetamodelFactory {

    /**
     * Creates an {@link AnnotatedEntityMetamodel} for the given entity type.
     *
     * @param entityType The type of the entity to create a metamodel for.
     * @param <C>        The type of the entity.
     * @return An {@link AnnotatedEntityMetamodel} for the given entity type.
     */
    <C> AnnotatedEntityMetamodel<C> createMetamodelForType(Class<C> entityType);
}
