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
import org.axonframework.messaging.core.Message;
import org.axonframework.modelling.EntityIdResolver;
import org.axonframework.modelling.FallbackEntityIdResolver;
import org.axonframework.modelling.MetadataEntityIdResolver;
import org.axonframework.modelling.annotation.AnnotationBasedEntityIdResolver;
import org.axonframework.modelling.annotation.EntityIdResolverDefinition;
import org.axonframework.modelling.entity.EntityMetamodel;

import java.util.Objects;

/**
 * An {@link EntityIdResolverDefinition} resolving an entity's identifier from a command's {@code @TargetEntityId}
 * payload field when present, falling back to the
 * {@link AggregateDeadlineCommandTranslator#DESCRIPTOR_BASED_ID} metadata entry
 * {@link AggregateDeadlineCommandTranslator} writes onto every command it dispatches.
 * <p>
 * A migrated aggregate's deadline {@link Message#payload()} typically carries no usable identifier of its own, so an
 * entity reached through a translated deadline needs this fallback to be resolvable at all.
 * {@link AggregateDeadlineEntityIdResolverConfigurationEnhancer} registers this class, constructed through
 * {@link #AggregateDeadlineEntityIdResolverDefinition()}, as the application-wide {@link EntityIdResolverDefinition}
 * default. This ensures {@link String}-based aggregate identifiers are resolved out of the box.
 * <p>
 * An application whose entity identifier is a different type supplies its own {@link MetadataEntityIdResolver} through
 * {@link #AggregateDeadlineEntityIdResolverDefinition(MetadataEntityIdResolver)} instead. The
 * {@link MetadataEntityIdResolver#forKey(String, Class, org.axonframework.conversion.Converter)} factory method
 * allows to set the required identifier type and a {@link org.axonframework.conversion.Converter} to correctly
 * convert the metadata value to an entity identifier.
 *
 * @author Steven van Beelen
 * @see AggregateDeadlineCommandTranslator
 * @see AggregateDeadlineEntityIdResolverConfigurationEnhancer
 * @see MetadataEntityIdResolver
 * @see FallbackEntityIdResolver
 * @since 5.4.0
 */
public class AggregateDeadlineEntityIdResolverDefinition implements EntityIdResolverDefinition {

    private final MetadataEntityIdResolver<?> metadataEntityIdResolver;

    /**
     * Initializes the definition with a {@link MetadataEntityIdResolver} resolving
     * {@link AggregateDeadlineCommandTranslator#DESCRIPTOR_BASED_ID} as a {@link String} identifier,
     * unconverted.
     * <p>
     * Use {@link #AggregateDeadlineEntityIdResolverDefinition(MetadataEntityIdResolver)} instead when the entity's
     * identifier is not a {@link String}, or when the aggregate identifier is written under a different metadata key.
     */
    public AggregateDeadlineEntityIdResolverDefinition() {
        this(MetadataEntityIdResolver.forKey(AggregateDeadlineCommandTranslator.DESCRIPTOR_BASED_ID));
    }

    /**
     * Initializes the definition with the given {@code metadataEntityIdResolver}, used as the fallback for a command
     * whose payload carries no {@code @TargetEntityId}.
     *
     * @param metadataEntityIdResolver the {@link MetadataEntityIdResolver} to fall back to
     */
    public AggregateDeadlineEntityIdResolverDefinition(MetadataEntityIdResolver<?> metadataEntityIdResolver) {
        this.metadataEntityIdResolver = Objects.requireNonNull(
                metadataEntityIdResolver, "The MetadataEntityIdResolver may not be null."
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E, ID> EntityIdResolver<ID> createIdResolver(
            Class<E> entityType,
            Class<ID> idType,
            EntityMetamodel<E> entityMetamodel,
            Configuration configuration
    ) {
        return new FallbackEntityIdResolver<>(
                new AnnotationBasedEntityIdResolver<>(),
                (EntityIdResolver<ID>) metadataEntityIdResolver
        );
    }
}
