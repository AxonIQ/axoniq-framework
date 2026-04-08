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

package org.axonframework.modelling.configuration;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.modelling.entity.EntityMetamodel;
import org.axonframework.modelling.entity.EntityMetamodelBuilder;

/**
 * Functional interface used to build an {@link EntityMetamodel} for a specific entity type, based on the provided
 * {@link Configuration} and {@link EntityMetamodelBuilder}.
 * <p>
 * Users supplying this can choose to disregard the provided {@link EntityMetamodelBuilder} and build their own
 * {@link EntityMetamodel} instance, or use the builder to configure the metamodel as they see fit.
 *
 * @param <E> The type of entity for which the metamodel is being built.
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@FunctionalInterface
public interface EntityMetamodelConfigurationBuilder<E> {

    /**
     * Builds an {@link EntityMetamodel} for the given entity type, using the provided {@link Configuration} and
     * {@link EntityMetamodelBuilder}.
     *
     * @param configuration The configuration to use for building the metamodel.
     * @param builder       The builder to use for configuring the metamodel.
     * @return The built {@link EntityMetamodel}.
     */
    EntityMetamodel<E> build(Configuration configuration, EntityMetamodelBuilder<E> builder);
}
