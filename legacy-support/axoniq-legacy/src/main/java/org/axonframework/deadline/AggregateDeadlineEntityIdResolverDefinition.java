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

import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.core.Message;
import org.axonframework.modelling.EntityIdResolver;
import org.axonframework.modelling.FallbackEntityIdResolver;
import org.axonframework.modelling.MetadataEntityIdResolver;
import org.axonframework.modelling.annotation.AnnotationBasedEntityIdResolver;
import org.axonframework.modelling.annotation.EntityIdResolverDefinition;
import org.axonframework.modelling.entity.EntityMetamodel;

/**
 * An {@link EntityIdResolverDefinition} resolving an entity's identifier from a command's {@code @TargetEntityId}
 * payload field when present, falling back to the {@link #DESCRIPTOR_BASED_ID} metadata entry
 * {@link AggregateDeadlineCommandTranslator} writes onto every command it dispatches.
 * <p>
 * A migrated aggregate's deadline {@link Message#payload()} typically carries no usable identifier of its own, so an
 * entity reached through a translated deadline needs this fallback to be resolvable at all.
 * {@link AggregateDeadlineEntityIdResolverConfigurationEnhancer} registers this class as the application-wide
 * {@link EntityIdResolverDefinition} default. Every entity gets its own {@link MetadataEntityIdResolver}, built from
 * the {@code idType} and {@link Configuration} {@link #createIdResolver(Class, Class, EntityMetamodel, Configuration)}
 * is given, converting the metadata value, which <b>always</b> is a {@link String}, with the {@link GeneralConverter}
 * the {@code Configuration} provides. This is not limited to a {@link String} identifier, but whether resolution
 * succeeds for a different {@code idType} depends on that {@link GeneralConverter}: the default, Jackson-based one
 * converts a type like {@link Long} out of the box, but not every identifier type round-trips through it without a
 * dedicated conversion being registered. A type such as {@link java.util.UUID}, for example, needs a
 * {@link GeneralConverter} able to parse an unquoted {@link String} into one.
 * <p>
 * This identifier predicament stems from the fact that Axon Framework 4 stored the {@link #toString()} value of
 * aggregate identifiers. Hence, that's the value being returned in the metadata, not a converted format.
 * <p>
 * This definition only takes effect for an {@code @EventSourcedEntity}/{@code @EventSourced} entity, the only kind
 * consulting a {@code Configuration}-registered {@link EntityIdResolverDefinition} default. A migrated state-stored
 * aggregate (a typical Axon Framework 4 JPA aggregate) or a declaratively configured entity module never reaches this
 * fallback: neither consults this override point, so a deadline translated for either still fails with an
 * {@code EntityIdResolutionException} unless that entity's own command handler payload carries a usable identifier.
 *
 * @author Steven van Beelen
 * @see AggregateDeadlineCommandTranslator
 * @see AggregateDeadlineEntityIdResolverConfigurationEnhancer
 * @see MetadataEntityIdResolver
 * @see FallbackEntityIdResolver
 * @since 5.4.0
 */
public class AggregateDeadlineEntityIdResolverDefinition implements EntityIdResolverDefinition {

    /**
     * The key under which an aggregate's identifier is expected as a fallback entity identifier, as written by
     * {@link AggregateDeadlineCommandTranslator} onto every command it dispatches.
     */
    public static final String DESCRIPTOR_BASED_ID = "scope-descriptor-based-entity-identifier";

    @Override
    public <E, ID> EntityIdResolver<ID> createIdResolver(
            Class<E> entityType,
            Class<ID> idType,
            EntityMetamodel<E> entityMetamodel,
            Configuration configuration
    ) {
        return new FallbackEntityIdResolver<>(
                new AnnotationBasedEntityIdResolver<>(),
                MetadataEntityIdResolver.forKey(
                        DESCRIPTOR_BASED_ID, idType, configuration.getComponent(GeneralConverter.class)
                )
        );
    }
}
