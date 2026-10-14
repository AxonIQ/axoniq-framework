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
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.EntityIdResolutionException;
import org.axonframework.modelling.EntityIdResolver;
import org.axonframework.modelling.FallbackEntityIdResolver;
import org.axonframework.modelling.MetadataEntityIdResolver;
import org.axonframework.modelling.annotation.AnnotationBasedEntityIdResolver;
import org.axonframework.modelling.annotation.EntityIdResolverDefinition;
import org.axonframework.modelling.entity.EntityMetamodel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * the {@code Configuration} provides. The default, Jackson-based {@link GeneralConverter} converts a type like
 * {@link Long} out of the box, but not every identifier type round-trips through it. A {@link java.util.UUID}, for
 * example, would need a {@link GeneralConverter} able to parse an unquoted {@link String}. When the conversion fails,
 * the raw {@link String} value is used instead. That value still finds the entity's events, since the default event
 * tag uses the identifier's {@code toString()} value as well. Only an {@code @EntityCreator} that injects the
 * identifier then fails to match, as it expects the converted identifier type.
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

    private static final Logger logger = LoggerFactory.getLogger(AggregateDeadlineEntityIdResolverDefinition.class);

    @Override
    public <E, ID> EntityIdResolver<ID> createIdResolver(
            Class<E> entityType,
            Class<ID> idType,
            EntityMetamodel<E> entityMetamodel,
            Configuration configuration
    ) {
        return new FallbackEntityIdResolver<>(
                new AnnotationBasedEntityIdResolver<>(),
                new ConvertingOrRawMetadataEntityIdResolver<>(
                        MetadataEntityIdResolver.forKey(
                                DESCRIPTOR_BASED_ID, idType, configuration.getComponent(GeneralConverter.class)
                        )
                )
        );
    }

    /**
     * An {@link EntityIdResolver} resolving the {@link #DESCRIPTOR_BASED_ID} metadata entry through the given
     * converting {@link MetadataEntityIdResolver}, falling back to the raw {@link String} value when the converter
     * cannot convert it into the entity's identifier type.
     * <p>
     * The raw {@code String} still finds the entity's events, since the default event tag uses the identifier's
     * {@code toString()} value as well. Hence, an entity whose {@code @EntityCreator} does not inject the identifier is
     * reached through a translated deadline regardless of its identifier type.
     *
     * @param <ID> the type of identifier to resolve
     */
    private static final class ConvertingOrRawMetadataEntityIdResolver<ID>
            implements EntityIdResolver<ID>, DescribableComponent {

        private final EntityIdResolver<ID> converting;
        private final EntityIdResolver<String> raw = MetadataEntityIdResolver.forKey(DESCRIPTOR_BASED_ID);

        private ConvertingOrRawMetadataEntityIdResolver(EntityIdResolver<ID> converting) {
            this.converting = converting;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ID resolve(Message message, ProcessingContext context) throws EntityIdResolutionException {
            try {
                return converting.resolve(message, context);
            } catch (ConversionException e) {
                logger.debug("Unable to convert the [{}] metadata entry into the entity's identifier type. "
                                     + "Falling back to the raw String value.", DESCRIPTOR_BASED_ID, e);
                return (ID) raw.resolve(message, context);
            }
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("converting", converting);
            descriptor.describeProperty("raw", raw);
        }
    }
}
